"""Scan a captured base dir, extract time_us from every record, and report
ranges + gaps. Use this to confirm the cursor-resume behavior:

- Run 1 of a mode (no persisted cursor) starts at live tip → there will be
  a gap between any *prior* run's end and this run's start.
- Run 2 of a mode (resumed from cursor file) should be effectively
  contiguous with run 1's last record (≤ ~10s gap = one disk-batch window
  plus a small reconnect delay).

Usage:
    python analyze_gaps.py testing/data
    python analyze_gaps.py testing/data_everything
"""

import glob
import json
import sys
from datetime import datetime, timezone

import zstandard as zstd


def iter_time_us(base_dir: str):
    files = sorted(glob.glob(f"{base_dir}/**/*.jsonl.zst", recursive=True))
    print(f"Scanning {len(files)} files under {base_dir}")
    dctx = zstd.ZstdDecompressor()
    for path in files:
        with open(path, "rb") as fh:
            try:
                data = dctx.stream_reader(fh).read()
            except zstd.ZstdError as e:
                print(f"  ! {path}: {e}", file=sys.stderr)
                continue
        for line in data.splitlines():
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            t = rec.get("time_us")
            if isinstance(t, int):
                yield t


def fmt(us: int) -> str:
    return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc).isoformat()


def main(base_dir: str) -> None:
    times = sorted(set(iter_time_us(base_dir)))
    if not times:
        print("No records found.")
        return

    print(f"\nRecords: {len(times):,}")
    print(f"First:   {times[0]}  ({fmt(times[0])})")
    print(f"Last:    {times[-1]}  ({fmt(times[-1])})")
    span_s = (times[-1] - times[0]) / 1_000_000
    print(f"Span:    {span_s:.1f}s")

    # Gap detection: anything > 30s between consecutive records is suspicious
    # for a high-volume firehose; smaller is fine.
    GAP_THRESHOLD_S = 2.0
    print(f"\nGaps > {GAP_THRESHOLD_S}s between consecutive records:")
    found = 0
    for prev, curr in zip(times, times[1:]):
        delta_s = (curr - prev) / 1_000_000
        if delta_s > GAP_THRESHOLD_S:
            found += 1
            print(f"  GAP {delta_s:8.1f}s   {fmt(prev)}  →  {fmt(curr)}")
    if not found:
        print("  (none — looks contiguous)")

    # Coarse histogram: bucket by 10s, print one row per non-empty bucket.
    print("\nDensity (records per 10s bucket, ▇ = ~max):")
    BUCKET_US = 10_000_000
    buckets: dict[int, int] = {}
    for t in times:
        b = t // BUCKET_US
        buckets[b] = buckets.get(b, 0) + 1
    max_count = max(buckets.values())
    width = 50
    for b in sorted(buckets):
        count = buckets[b]
        bar = "▇" * max(1, int(width * count / max_count))
        ts = fmt(b * BUCKET_US)
        print(f"  {ts}  {count:6d}  {bar}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_gaps.py <base_dir>", file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1])
