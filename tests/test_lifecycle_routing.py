"""Tests for lifecycle-event routing in BlueskyArchiver.save_posts_async.

These tests exercise the disk-routing branch added when identity/account
events became first-class. They run without network access by feeding
hand-crafted records directly into save_posts_async and asserting which
files appear under a temporary working directory.

Run standalone:    python tests/test_lifecycle_routing.py
Run under pytest:  pytest tests/test_lifecycle_routing.py
"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.archiver import BlueskyArchiver


# A timestamp inside 2026-04-07 14:00 UTC, matching the sample data we
# captured from Jetstream. Using a fixed time keeps the asserted file
# paths deterministic.
TS_US = 1775572272912044
EXPECTED_DIR = Path("data_non_posts/2026-04/07")
EXPECTED_LIFECYCLE = EXPECTED_DIR / "lifecycle_20260407_14.jsonl.zst"
EXPECTED_RECORDS = EXPECTED_DIR / "records_20260407_14.jsonl.zst"


def _identity_event():
    return {
        "did": "did:plc:identityexample",
        "time_us": TS_US,
        "kind": "identity",
        "identity": {
            "did": "did:plc:identityexample",
            "seq": 1,
            "time": "2026-04-07T14:31:12.380Z",
        },
    }


def _account_event():
    return {
        "did": "did:plc:accountexample",
        "time_us": TS_US + 1,
        "kind": "account",
        "account": {
            "active": True,
            "did": "did:plc:accountexample",
            "seq": 2,
            "time": "2026-04-07T14:31:13.339Z",
        },
    }


def _commit_event():
    # Mirrors the raw Jetstream shape that archive_non_posts forwards
    # untouched (a non-post commit such as a like).
    return {
        "did": "did:plc:commitexample",
        "time_us": TS_US + 2,
        "kind": "commit",
        "commit": {
            "rev": "abc",
            "operation": "create",
            "collection": "app.bsky.feed.like",
            "rkey": "xyz",
            "record": {"$type": "app.bsky.feed.like"},
        },
    }


def _read_jsonl(path: Path):
    import zstandard as zstd

    # Files may contain multiple concatenated zstd frames (one per batch
    # write), so use a streaming reader rather than one-shot decompress.
    import io

    with open(path, "rb") as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True)
        raw = io.TextIOWrapper(reader, encoding="utf-8").read()
    return [json.loads(line) for line in raw.splitlines() if line]


def _in_tmp_cwd(coro_factory):
    """Run an async test in an isolated working directory."""
    with tempfile.TemporaryDirectory() as tmp:
        prev = os.getcwd()
        os.chdir(tmp)
        try:
            asyncio.run(coro_factory(Path(tmp)))
        finally:
            os.chdir(prev)


async def _save_lifecycle_only(tmp: Path):
    archiver = BlueskyArchiver(archive_non_posts=True)
    await archiver.save_posts_async([_identity_event(), _account_event()])

    lifecycle = tmp / EXPECTED_LIFECYCLE
    records = tmp / EXPECTED_RECORDS

    assert lifecycle.exists(), f"expected {lifecycle} to exist"
    assert not records.exists(), f"unexpected {records} created"

    rows = _read_jsonl(lifecycle)
    assert len(rows) == 2, f"expected 2 lifecycle rows, got {len(rows)}"
    kinds = sorted(r["kind"] for r in rows)
    assert kinds == ["account", "identity"], kinds


async def _save_commit_only(tmp: Path):
    archiver = BlueskyArchiver(archive_non_posts=True)
    await archiver.save_posts_async([_commit_event()])

    lifecycle = tmp / EXPECTED_LIFECYCLE
    records = tmp / EXPECTED_RECORDS

    assert records.exists(), f"expected {records} to exist"
    assert not lifecycle.exists(), f"unexpected {lifecycle} created"

    rows = _read_jsonl(records)
    assert len(rows) == 1
    assert rows[0]["kind"] == "commit"


async def _save_mixed_batch(tmp: Path):
    archiver = BlueskyArchiver(archive_non_posts=True)
    await archiver.save_posts_async(
        [_identity_event(), _commit_event(), _account_event()]
    )

    lifecycle = tmp / EXPECTED_LIFECYCLE
    records = tmp / EXPECTED_RECORDS

    assert lifecycle.exists() and records.exists(), (
        "both lifecycle and records files should exist for a mixed batch"
    )

    lifecycle_rows = _read_jsonl(lifecycle)
    records_rows = _read_jsonl(records)

    assert len(lifecycle_rows) == 2
    assert len(records_rows) == 1
    assert sorted(r["kind"] for r in lifecycle_rows) == ["account", "identity"]
    assert records_rows[0]["kind"] == "commit"


# pytest discovery: zero-arg sync wrappers around the async coroutines.
def test_save_lifecycle_only():
    _in_tmp_cwd(_save_lifecycle_only)


def test_save_commit_only():
    _in_tmp_cwd(_save_commit_only)


def test_save_mixed_batch():
    _in_tmp_cwd(_save_mixed_batch)


if __name__ == "__main__":
    failures = 0
    for name, fn in [
        ("lifecycle-only batch", test_save_lifecycle_only),
        ("commit-only batch", test_save_commit_only),
        ("mixed batch", test_save_mixed_batch),
    ]:
        try:
            fn()
            print(f"  ok    {name}")
        except AssertionError as e:
            failures += 1
            print(f"  FAIL  {name}: {e}")
        except Exception as e:
            failures += 1
            print(f"  ERROR {name}: {type(e).__name__}: {e}")

    if failures:
        print(f"\n{failures} test(s) failed")
        sys.exit(1)
    print("\nall lifecycle routing tests passed")
