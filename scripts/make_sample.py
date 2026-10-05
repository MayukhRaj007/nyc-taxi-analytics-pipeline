"""Create the small, committed sample Parquet used by CI and local smoke tests.

It is a reproducible random sample of real January 2025 trips, so it keeps the
real data's quirks (refunds, zero-distance trips, odd timestamps) that the
cleaning rules and tests are meant to catch.

Usage: python scripts/make_sample.py [source.parquet] [rows]
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

DEFAULT_SOURCE = Path(os.environ.get("DATA_DIR", "data")) / "raw" / "yellow_tripdata_2025-01.parquet"
OUTPUT = Path("sample_data") / "yellow_tripdata_sample_2025-01.parquet"


def main() -> None:
    source = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SOURCE
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 3000
    OUTPUT.parent.mkdir(exist_ok=True)
    duckdb.execute(
        f"COPY (SELECT * FROM read_parquet('{source.as_posix()}') USING SAMPLE reservoir({rows} ROWS) REPEATABLE (42)) "
        f"TO '{OUTPUT.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    print(f"Wrote {OUTPUT} ({OUTPUT.stat().st_size / 1024:.0f} KB, {rows} rows)")


if __name__ == "__main__":
    main()
