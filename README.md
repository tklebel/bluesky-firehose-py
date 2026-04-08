# Bluesky Firehose Archiver

A Python library for collecting and archiving posts from the Bluesky social network using the [Jetstream API](https://github.com/bluesky-social/jetstream). This tool connects to Bluesky's firehose and saves posts in an organized file structure.

## Features

- Connects to Bluesky's Jetstream websocket API
- Three archiving modes:
  - Posts only (default)
  - All records (posts, likes, follows, etc.)
  - Non-posts only (everything except posts)
- Archives data in zstd-compressed JSONL format (`.jsonl.zst`), organized by date and hour
- Optional real-time post text streaming to stdout
- Automatic reconnection on connection loss
- Crash-safe resume: persists a per-mode cursor on every flush so a restart picks up where it left off
- Efficient batch processing and disk operations
- Debug mode for detailed logging
- Optional handle resolution (disabled by default)
- Playback support from specific timestamps

## Installation

### Manual Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/bluesky-firehose-archiver.git
```

2. Navigate to the project directory:
```bash
cd bluesky-firehose-archiver
```

3. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

4. Install dependencies:
```bash
pip install -r requirements.txt
```

### Docker Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/bluesky-firehose-archiver.git
cd bluesky-firehose-archiver
```

2. Build and run with Docker Compose:
```bash
# Build the image
docker-compose build

# Run bluesky-posts and bluesky-non-posts as separate containers
docker-compose up 

# Or run in the background
docker-compose up -d
```

## Usage

### Command Line Interface

The archiver supports three distinct modes of operation:

1. **Posts Only** (default):
```bash
python src/main.py
```
- Archives only posts (`app.bsky.feed.post` records)
- Saves to `data/` directory
- Files named `posts_YYYYMMDD_HH.jsonl.zst` for new posts, plus `post_updates_YYYYMMDD_HH.jsonl.zst` for deletes/updates
- Filenames use UTC, so paths are consistent between local and Docker runs

2. **All Records**:
```bash
python src/main.py --archive-all
```
- Archives all record types (posts, likes, follows, etc.)
- Saves to `data_everything/` directory
- Files named `records_YYYYMMDD_HH.jsonl.zst` (and `lifecycle_YYYYMMDD_HH.jsonl.zst` for identity/account events)
- Preserves complete record structure

3. **Non-Posts Only**:
```bash
python src/main.py --archive-non-posts
```
- Archives everything except posts
- Saves to `data_non_posts/` directory
- Files named `records_YYYYMMDD_HH.jsonl.zst` (and `lifecycle_YYYYMMDD_HH.jsonl.zst` for identity/account events)
- Useful for collecting only interactions and profile updates

Note: The `--archive-all` and `--archive-non-posts` modes cannot be used simultaneously.

#### Additional Options

```bash
python src/main.py [options]

Options:
  --username         Bluesky username (optional)
  --password         Bluesky password (optional)
  --debug           Enable debug output
  --stream          Stream post text to stdout in real-time
  --measure-rate    Track and display posts per minute rate
  --get-handles     Resolve handles while archiving (not recommended)
  --cursor          Unix microseconds timestamp to start playback from.
                    Overrides the auto-resume cursor file (default: resume
                    from <data-dir>/.cursor if present, else live tip)
```

### Library Usage

You can use the archiver in your Python code:

```python
from archiver import BlueskyArchiver
import asyncio

async def main():
    # Initialize with desired options
    archiver = BlueskyArchiver(
        debug=True,           # Enable debug logging
        stream=True,          # Stream posts to stdout
        measure_rate=True,    # Show collection rate
        archive_all=False,    # Default: posts only
        get_handles=False     # Don't resolve handles
    )
    
    try:
        # Start archiving
        await archiver.archive_posts()
    finally:
        # Ensure clean shutdown
        archiver.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## Data Storage

Records are saved as zstd-compressed JSONL (`.jsonl.zst`), organized by date and hour in different directories based on the archiving mode. Each file is a concatenation of independent zstd frames (one per disk batch), so standard zstd decoders read it as a single stream (`zstdcat file.jsonl.zst | jq .`). Each base dir also contains a hidden `.cursor` file used for crash-safe resume.

```
data/                      # Posts only mode (default)
  └── YYYY-MM/
      └── DD/
          ├── posts_YYYYMMDD_HH.jsonl.zst         # creates
          └── post_updates_YYYYMMDD_HH.jsonl.zst  # deletes / updates

data_everything/          # Archive all mode
  └── YYYY-MM/
      └── DD/
          └── records_YYYYMMDD_HH.jsonl.zst

data_non_posts/          # Non-posts mode
  └── YYYY-MM/
      └── DD/
          └── records_YYYYMMDD_HH.jsonl.zst
```

### Record Format

1. **Posts Only Mode** (default):
```json
{
    "handle": "user.bsky.social",
    "record": {
        "text": "Post content",
        "createdAt": "2024-03-15T01:23:45.678Z",
        ...
    },
    "rkey": "unique-record-key",
    "did": "did:plc:abcd...",
    "time_us": 1234567890
}
```

2. **Archive All & Non-Posts Modes**:
```json
{
    "did": "did:plc:abcd...",
    "time_us": 1234567890,
    "kind": "commit",
    "commit": {
        "rev": "...",
        "operation": "create",
        "collection": "app.bsky.feed.like",  // or other collection types
        "rkey": "...",
        "record": { ... }
    }
}
```

### Playback & Crash-Safe Resume

By default the archiver picks up where the previous run left off. After every successful disk flush it writes the maximum `time_us` of that batch to a per-mode cursor file:

- `data/.cursor` (posts mode)
- `data_non_posts/.cursor` (`--archive-non-posts`)
- `data_everything/.cursor` (`--archive-all`)

On startup, if no explicit `--cursor` was passed, the archiver reads this file and resumes from that timestamp. Because the cursor is written from the *post-flush* side, a crash (kill -9, OOM, container restart) replays at most one disk-batch window (~10s) — Jetstream serves the replay and downstream dedup on `time_us` collapses it.

To override (e.g. first-time bootstrap, or to backfill from a known point), pass `--cursor` explicitly:

```bash
python src/main.py --cursor 1725911162329308
```

The cursor is a Unix timestamp in microseconds — find one in any saved record's `time_us` field. Explicit `--cursor` always wins over the persisted file.

To verify resume behavior on captured data, see `interactive_testing/analyze_gaps.py`, which scans a base dir's `.jsonl.zst` files and reports any time_us gaps.

## Project Structure

```
├── src/
│   ├── main.py             # Entry point and CLI interface
│   └── archiver.py         # Core archiving logic
├── tests/                  # Pytest unit tests (test_archiver.py, test_lifecycle_routing.py)
├── interactive_testing/    # Manual / exploratory test utilities
│   └── analyze_gaps.py     # Scan archived .jsonl.zst for time_us gaps (cursor resume validation)
├── data/                   # Archived posts storage (posts mode)
├── data_non_posts/         # Archived non-post records (--archive-non-posts)
├── data_everything/        # Archived everything (--archive-all)
├── Dockerfile              # Container image definition
├── docker-compose.yml      # Two-service deployment (bluesky-posts + bluesky-non-posts)
├── docker-entrypoint.sh    # Container entrypoint
├── requirements.txt        # Project dependencies
└── README.md               # This file
```

## License

MIT License
