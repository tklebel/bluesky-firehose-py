import websockets
import asyncio
import json
from datetime import datetime, timezone
import os
from atproto import Client
from typing import Dict, Optional, List, AsyncGenerator
from asyncio import Queue
import logging
import aiofiles
import sys
import zstandard as _zstd

# One reusable compressor. ZstdCompressor.compress() is safe to call
# repeatedly; we only have a single disk worker task today, so no locking
# is needed.
_ZSTD_CCTX = _zstd.ZstdCompressor(level=6)

# Disk-write batching. Coalesce records the disk worker pulls off the
# processed queue so each zstd frame contains a meaningful payload —
# single-record frames defeat compression entirely.
DISK_BATCH_MAX_RECORDS = 5000
DISK_BATCH_WINDOW_S = 10.0


async def _append_zst(path: str, lines: List[str]) -> None:
    """Append a batch of JSONL lines to ``path`` as a single zstd frame.

    A .jsonl.zst file produced this way is a concatenation of independent
    frames, one per batch. Standard zstd decoders read it as one logical
    stream. Compression ratio is somewhat worse than a single big frame,
    but each batch is self-contained, so a crash mid-write can at worst
    corrupt the trailing frame.
    """
    if not lines:
        return
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    frame = _ZSTD_CCTX.compress(payload)
    async with aiofiles.open(path, "ab") as f:
        await f.write(frame)


class BlueskyArchiver:
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        debug: bool = False,
        stream: bool = False,
        measure_rate: bool = False,
        get_handles: bool = False,
        cursor: Optional[int] = None,  # Unix microseconds timestamp
        archive_all: bool = False,  # Flag to archive all records
        archive_non_posts: bool = False,  # Flag to archive everything except posts
        jetstream_url: Optional[str] = None,
    ):
        """Initialize the Bluesky Archiver."""
        # Configure logging
        logging.getLogger("websockets").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("httpx").setLevel(logging.WARNING)

        if archive_all and archive_non_posts:
            raise ValueError(
                "Cannot use both archive_all and archive_non_posts modes simultaneously"
            )

        self.debug = debug
        self.stream = stream
        self.measure_rate = measure_rate
        self.get_handles = get_handles
        self.cursor = cursor
        self.post_count = 0
        self.posts_saved = 0
        self.start_time = None
        self.running = True

        # Initialize client
        self.client = Client()
        if username and password:
            self.client.login(username, password)

        # Jetstream endpoint precedence: explicit arg → env var → default.
        # Different Jetstream instances stamp events with their own time_us
        # (the time *that* server received the event from the relay), so
        # cursors are portable across instances but not bit-identical —
        # expect a few seconds of replay when switching.
        self.uri = (
            jetstream_url
            or os.environ.get("JETSTREAM_URL")
            or "wss://jetstream2.us-east.bsky.network/subscribe"
        )
        self.handle_cache: Dict[str, str] = {}
        self.resolving_dids: set = set()  # Track DIDs currently being resolved
        self.handle_semaphore = asyncio.Semaphore(
            10
        )  # Limit concurrent handle resolutions

        # Initialize queues
        self.raw_queue: Queue = Queue()  # Posts from websocket with None handles
        self.processed_queue: Queue = Queue()  # Posts with resolved handles

        # Initialize background tasks
        self.disk_task: asyncio.Task = None
        self.handle_task: asyncio.Task = None
        self.websocket_task: asyncio.Task = None

        # Set up logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        logging.info(f"Using Jetstream endpoint: {self.uri}")

        self.archive_all = archive_all
        self.archive_non_posts = archive_non_posts

        # Per-mode cursor file: each mode writes to its own base dir, so
        # each gets its own resume cursor and they don't clobber each other.
        if archive_all:
            self.cursor_file = "data_everything/.cursor"
        elif archive_non_posts:
            self.cursor_file = "data_non_posts/.cursor"
        else:
            self.cursor_file = "data/.cursor"
        self._last_persisted_cursor: Optional[int] = None

        # Auto-resume: if no explicit --cursor was passed, try to pick up
        # where the previous run left off. Explicit cursor always wins.
        if self.cursor is None:
            try:
                with open(self.cursor_file, "r") as f:
                    persisted = int(f.read().strip())
                self.cursor = persisted
                self._last_persisted_cursor = persisted
                human = datetime.fromtimestamp(
                    persisted / 1_000_000, tz=timezone.utc
                ).isoformat()
                logging.info(
                    f"Resuming from persisted cursor {persisted} ({human})"
                )
            except FileNotFoundError:
                logging.info(
                    f"No persisted cursor at {self.cursor_file}, starting at live tip"
                )
            except Exception as e:
                logging.warning(
                    f"Could not read cursor file {self.cursor_file}: {e}. Starting at live tip"
                )
        else:
            logging.info(f"Using explicit cursor {self.cursor} (overrides any persisted cursor)")

    def _persist_cursor(self, batch: List[dict]) -> None:
        """Write the max time_us in ``batch`` to the cursor file atomically.

        Called from disk_worker after a successful flush, so the persisted
        value is always ≤ what is durable on disk. On crash + restart, at
        most one batch worth of records (~10s) is replayed; Jetstream serves
        the replay and downstream dedup collapses it.
        """
        try:
            max_us = max(
                (r["time_us"] for r in batch if isinstance(r.get("time_us"), int)),
                default=None,
            )
            if max_us is None:
                return
            if self._last_persisted_cursor is not None and max_us <= self._last_persisted_cursor:
                return
            os.makedirs(os.path.dirname(self.cursor_file), exist_ok=True)
            tmp = f"{self.cursor_file}.tmp"
            with open(tmp, "w") as f:
                f.write(str(max_us))
            os.replace(tmp, self.cursor_file)
            self._last_persisted_cursor = max_us
        except Exception as e:
            logging.error(f"🔴 Failed to persist cursor: {e}")

    async def get_handle(self, did: str) -> Optional[str]:
        """Retrieve handle for a given DID."""
        if did in self.handle_cache:
            return self.handle_cache[did]
        # If this DID is already being resolved, wait a bit and check cache again
        if did in self.resolving_dids:
            await asyncio.sleep(0.1)
            return self.handle_cache.get(did)

        self.resolving_dids.add(did)
        try:
            async with self.handle_semaphore:
                response = await asyncio.to_thread(
                    self.client.com.atproto.repo.describe_repo, {"repo": did}
                )
                handle = response.handle
                self.handle_cache[did] = handle
                return handle
        except Exception as e:
            logging.error(f"🔴 Error getting handle for {did}: {e}")
            return None
        finally:
            self.resolving_dids.remove(did)

    async def save_posts_async(self, posts: List[dict]):
        if self.measure_rate and self.start_time is None:
            self.start_time = datetime.now()

        """Asynchronously save posts to JSONL files, organized by hour."""
        if self.archive_all or self.archive_non_posts:
            # Group records by destination file so each file is opened and
            # compressed once per batch instead of once per record.
            base_dir = "data_everything" if self.archive_all else "data_non_posts"
            records_by_path: Dict[str, List[str]] = {}
            for record in posts:
                post_time = datetime.fromtimestamp(record["time_us"] / 1_000_000, tz=timezone.utc)
                date_dir = post_time.strftime("%Y-%m/%d")
                if record.get("kind") in ("identity", "account"):
                    hour_filename = post_time.strftime("lifecycle_%Y%m%d_%H.jsonl.zst")
                else:
                    hour_filename = post_time.strftime("records_%Y%m%d_%H.jsonl.zst")
                full_path = f"{base_dir}/{date_dir}/{hour_filename}"
                records_by_path.setdefault(full_path, []).append(
                    json.dumps(record, ensure_ascii=False)
                )

            for full_path, lines in records_by_path.items():
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                await _append_zst(full_path, lines)

            self.posts_saved += len(posts)

            if self.measure_rate:
                self.post_count += len(posts)
                now = datetime.now()
                if (
                    (now - self.last_rate_check).total_seconds() >= 10
                    if hasattr(self, "last_rate_check")
                    else True
                ):
                    elapsed_minutes = (now - self.start_time).total_seconds() / 60
                    rate = self.post_count / elapsed_minutes
                    estimated_daily = rate * 60 * 24
                    logging.info(
                        f"Current rate: {rate:.1f} records/minute (est. {int(estimated_daily):,} records/day)"
                    )
                    self.last_rate_check = now
            return

        posts_by_hour = {}
        for post in posts:
            post_time = datetime.fromtimestamp(post["time_us"] / 1_000_000, tz=timezone.utc)
            date_dir = post_time.strftime("%Y-%m/%d")
            operation = post.get("operation", "create")
            if operation == "create":
                hour_filename = post_time.strftime("posts_%Y%m%d_%H.jsonl.zst")
            else:
                hour_filename = post_time.strftime("post_updates_%Y%m%d_%H.jsonl.zst")
            full_path = f"data/{date_dir}/{hour_filename}"

            posts_by_hour.setdefault(full_path, []).append(post)

        for full_path, hour_posts in posts_by_hour.items():
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            await _append_zst(
                full_path,
                [json.dumps(p, ensure_ascii=False) for p in hour_posts],
            )
            self.posts_saved += len(hour_posts)

        if self.measure_rate:
            self.post_count += len(posts)
            now = datetime.now()
            if (
                (now - self.last_rate_check).total_seconds() >= 10
                if hasattr(self, "last_rate_check")
                else True
            ):
                elapsed_minutes = (now - self.start_time).total_seconds() / 60
                rate = self.post_count / elapsed_minutes
                estimated_daily = rate * 60 * 24
                logging.info(
                    f"Current rate: {rate:.1f} posts/minute (est. {int(estimated_daily):,} posts/day)"
                )
                self.last_rate_check = now

    async def get_handle_and_update(self, post: dict):
        """Get handle for a post and update post and queue when done."""
        handle = await self.get_handle(post["did"])
        post["handle"] = handle
        await self.processed_queue.put([post])

    async def handle_processor(self):
        """Process posts from raw_queue, resolve handles, and put in processed_queue."""
        while self.running:
            try:
                posts = await self.raw_queue.get()
                # Lifecycle events (identity/account) carry no post-shaped
                # payload and must skip handle resolution entirely.
                if posts and posts[0].get("kind") in ("identity", "account"):
                    await self.processed_queue.put(posts)
                    self.raw_queue.task_done()
                    continue
                if self.get_handles:
                    # Process all posts in parallel
                    tasks = []
                    for post in posts:
                        if post["did"] not in self.handle_cache:
                            tasks.append(self.get_handle_and_update(post))
                        else:
                            # If handle is in cache, send directly to processed queue
                            post["handle"] = self.handle_cache[post["did"]]
                            tasks.append(self.processed_queue.put([post]))

                    # Wait for all handle resolutions and queue updates to complete
                    if tasks:
                        await asyncio.gather(*tasks)
                else:
                    # Skip handle resolution, just forward to processed queue
                    await self.processed_queue.put(posts)

                self.raw_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"🔴 Error in handle processor: {e}")

    async def disk_worker(self) -> None:
        """Background task to handle disk operations."""
        while self.running:
            try:
                first = await self.processed_queue.get()
                batch = list(first)
                self.processed_queue.task_done()

                # Coalesce additional queued records up to a size or
                # time budget so each zstd frame contains a meaningful
                # payload instead of one frame per Jetstream message.
                # On deadline / size hit we break out of the inner loop
                # and fall through to save_posts_async below — nothing
                # in `batch` is ever discarded.
                loop = asyncio.get_event_loop()
                deadline = loop.time() + DISK_BATCH_WINDOW_S
                while self.running and len(batch) < DISK_BATCH_MAX_RECORDS:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        more = await asyncio.wait_for(
                            self.processed_queue.get(), timeout=remaining
                        )
                    except asyncio.TimeoutError:
                        break
                    batch.extend(more)
                    self.processed_queue.task_done()

                await self.save_posts_async(batch)
                self._persist_cursor(batch)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"🔴 Error in disk worker: {e}")

    async def archive_websocket_listener(self):
        """Continuously listen to the WebSocket and enqueue posts."""
        # Server-side scope: restrict to Bluesky's namespace. This is the
        # primary safeguard against incidentally ingesting the hundreds of
        # unrelated third-party atproto apps that share the firehose.
        # Posts-only mode narrows further to a single NSID; archive_all /
        # archive_non_posts use the prefix so new app.bsky.* record types
        # (e.g. actor.status, notification.declaration) are captured as
        # Bluesky ships them, without requiring code changes.
        if self.archive_all or self.archive_non_posts:
            wanted_collections = ["app.bsky.*"]
        else:
            wanted_collections = ["app.bsky.feed.post"]

        while self.running:
            try:
                query_parts = [f"wantedCollections={c}" for c in wanted_collections]
                if self.cursor:
                    if self.debug:
                        logging.debug(f"Starting playback from cursor: {self.cursor}")
                    query_parts.append(f"cursor={self.cursor}")

                url = f"{self.uri}?{'&'.join(query_parts)}"

                async with websockets.connect(url) as archive_websocket:
                    if self.debug:
                        logging.debug("🟢 Connected to firehose for archiving")
                    async for message in archive_websocket:
                        if not self.running:
                            break
                        data = json.loads(message)

                        # Advance in-memory cursor for in-process reconnects.
                        # The *persisted* resume cursor is written from the
                        # post-flush side in disk_worker via _persist_cursor,
                        # so a crash replays at most one batch (~10s).
                        self.cursor = data.get("time_us")

                        kind = data.get("kind")

                        # Lifecycle (identity/account) events are NOT scoped
                        # by wantedCollections — jetstream sends them for
                        # every DID on the network regardless of app. We
                        # keep them in the broader archive modes: they only
                        # record that *something* changed (handle rotation,
                        # account takedown) without revealing bio content
                        # or other plaintext, which is acceptable scope.
                        if kind in ("identity", "account"):
                            if self.archive_all or self.archive_non_posts:
                                await self.raw_queue.put([data])
                            continue

                        if kind != "commit":
                            continue

                        commit = data.get("commit", {})

                        # Defense-in-depth: if the server ever sends a
                        # commit outside our requested namespace (protocol
                        # bug, relay misconfig), drop it loudly instead of
                        # silently persisting it.
                        collection = commit.get("collection", "")
                        if not collection.startswith("app.bsky."):
                            logging.warning(
                                f"🔴 Unexpected collection on wire: {collection!r} — dropping"
                            )
                            continue

                        # Check if it's a post
                        is_post = collection == "app.bsky.feed.post"

                        # Handle different archiving modes
                        if self.archive_all:
                            await self.raw_queue.put([data])
                        elif self.archive_non_posts:
                            if not is_post:
                                await self.raw_queue.put([data])
                        else:  # Posts only mode
                            if is_post:
                                did = data.get("did")
                                post_record = {
                                    "handle": None,
                                    "operation": commit.get("operation"),
                                    "record": commit.get("record"),
                                    "rkey": commit.get("rkey"),
                                    "did": did,
                                    "time_us": data.get("time_us"),
                                }
                                await self.raw_queue.put([post_record])

                        if (
                            self.stream
                            and is_post
                            and "text" in commit.get("record", {})
                        ):
                            sys.stdout.write(f"🖊️: {commit['record']['text']}\n")
                            sys.stdout.flush()

            except asyncio.CancelledError:
                break  # Exit cleanly on cancellation
            except websockets.exceptions.ConnectionClosed as e:
                if self.running:  # Only try to reconnect if we're still running
                    logging.warning(
                        f"🔴 Connection closed: {e}. Reconnecting in 5 seconds..."
                    )
                    await asyncio.sleep(5)
            except Exception as e:
                if self.running:
                    logging.error(
                        f"🔴 Unexpected error: {e}. Reconnecting in 5 seconds..."
                    )
                    await asyncio.sleep(5)

    async def archive_posts(self):
        """Start archiving posts with continuous WebSocket listening."""
        # Start background tasks
        self.websocket_task = asyncio.create_task(self.archive_websocket_listener())
        self.handle_task = asyncio.create_task(self.handle_processor())
        self.disk_task = asyncio.create_task(self.disk_worker())

        # Wait for background tasks to complete
        await asyncio.gather(self.websocket_task, self.handle_task, self.disk_task)

    async def cleanup(self):
        """Clean up background tasks."""
        self.running = False
        if not hasattr(self, "_cleanup_started"):
            self._cleanup_started = True
        else:
            return

        logging.info("Shutting down... Saving remaining posts...")

        # First, save any remaining posts in the queues
        remaining_posts = 0
        # Process any remaining raw posts
        while not self.raw_queue.empty():
            try:
                posts = self.raw_queue.get_nowait()
                # Process all posts in parallel
                tasks = []
                for post in posts:
                    if post["did"] not in self.handle_cache:
                        tasks.append(self.get_handle_and_update(post))
                    else:
                        # If handle is in cache, send directly to processed queue
                        post["handle"] = self.handle_cache[post["did"]]
                        tasks.append(self.processed_queue.put([post]))

                # Wait for all handle resolutions and queue updates to complete
                if tasks:
                    await asyncio.gather(*tasks)
                self.raw_queue.task_done()
            except asyncio.QueueEmpty:
                break

        # Save processed posts
        while not self.processed_queue.empty():
            try:
                posts = self.processed_queue.get_nowait()
                remaining_posts += len(posts)
                await self.save_posts_async(posts)
                self.processed_queue.task_done()
            except asyncio.QueueEmpty:
                break

        if remaining_posts > 0:
            logging.info(f"Saved {remaining_posts} remaining posts during shutdown")

        tasks = []
        for task in [self.websocket_task, self.disk_task, self.handle_task]:
            if task and not task.done():
                task.cancel()
                tasks.append(task)

        if tasks:
            try:
                # Wait for tasks to complete with a timeout
                await asyncio.wait(tasks, timeout=5.0)
            except asyncio.CancelledError:
                pass  # Ignore cancellation during cleanup
            except Exception as e:
                logging.error(f"Error during cleanup: {e}")

    def stop(self):
        """Stop the archiver gracefully."""
        self.running = False  # Ensure running is set to False first

        # Create cleanup task with a timeout
        async def cleanup_with_timeout():
            try:
                await asyncio.wait_for(self.cleanup(), timeout=5.0)
            except asyncio.TimeoutError:
                logging.warning("Cleanup timed out after 5 seconds")
                # Force exit after timeout
                import sys

                sys.exit(0)
            except Exception as e:
                logging.error(f"Error during cleanup: {e}")
                sys.exit(1)

        # Run cleanup and wait for it
        asyncio.create_task(cleanup_with_timeout())

        # Only show stats once during cleanup
        if (
            self.measure_rate
            and self.start_time
            and not hasattr(self, "_cleanup_started")
        ):
            elapsed_minutes = (datetime.now() - self.start_time).total_seconds() / 60
            if elapsed_minutes > 0:
                rate = self.post_count / elapsed_minutes
                estimated_daily = rate * 60 * 24
                logging.info(f"Final rate: {rate:.1f} posts/minute")
                logging.info(
                    f"Estimated daily volume: {int(estimated_daily):,} posts/day"
                )
                logging.info(
                    f"Total posts collected: {self.post_count:,} (saved: {self.posts_saved:,})"
                )
                if self.post_count != self.posts_saved:
                    logging.warning(
                        f"⚠️ Discrepancy: {self.post_count - self.posts_saved:,} posts were collected but not saved"
                    )
