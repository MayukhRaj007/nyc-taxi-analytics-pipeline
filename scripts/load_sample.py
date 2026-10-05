"""Load the committed sample file into DuckDB (used by CI and quick local dev).

Usage: python scripts/load_sample.py [db_path]
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from taxi_lib.load import load_month

logging.basicConfig(level=logging.INFO, format="%(message)s")

SAMPLE = Path("sample_data") / "yellow_tripdata_sample_2025-01.parquet"


def main() -> None:
    db_path = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DUCKDB_PATH", "warehouse/taxi.duckdb")
    rows = load_month(SAMPLE, "2025-01", db_path)
    print(f"Loaded {rows} sample rows into {db_path}")


if __name__ == "__main__":
    main()
