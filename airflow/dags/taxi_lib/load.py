"""Load a month of Parquet into DuckDB ``raw.yellow_trips`` idempotently."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from taxi_lib.months import month_bounds

log = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
RAW_TABLE = "yellow_trips"

# Canonical raw layout. TLC changes the schema over time (e.g. cbd_congestion_fee
# appears in 2025, Airport_fee changes case), so we pin names and types here and
# fill anything a given file lacks with NULL. Raw stays faithful to the source.
RAW_COLUMNS: dict[str, str] = {
    "VendorID": "BIGINT",
    "tpep_pickup_datetime": "TIMESTAMP",
    "tpep_dropoff_datetime": "TIMESTAMP",
    "passenger_count": "BIGINT",
    "trip_distance": "DOUBLE",
    "RatecodeID": "BIGINT",
    "store_and_fwd_flag": "VARCHAR",
    "PULocationID": "BIGINT",
    "DOLocationID": "BIGINT",
    "payment_type": "BIGINT",
    "fare_amount": "DOUBLE",
    "extra": "DOUBLE",
    "mta_tax": "DOUBLE",
    "tip_amount": "DOUBLE",
    "tolls_amount": "DOUBLE",
    "improvement_surcharge": "DOUBLE",
    "total_amount": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
    "Airport_fee": "DOUBLE",
    "cbd_congestion_fee": "DOUBLE",
}


def _create_table(con: duckdb.DuckDBPyConnection) -> None:
    cols = ", ".join(f'"{name}" {dtype}' for name, dtype in RAW_COLUMNS.items())
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{RAW_TABLE} ({cols}, source_month DATE, loaded_at TIMESTAMP)"
    )


def _select_list(con: duckdb.DuckDBPyConnection, parquet: str) -> str:
    """Build a SELECT that maps the file's columns (case-insensitive) onto RAW_COLUMNS."""
    found = {
        row[0].lower(): row[0]
        for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [parquet]).fetchall()
    }
    parts = []
    for name, dtype in RAW_COLUMNS.items():
        src = found.get(name.lower())
        expr = f'CAST("{src}" AS {dtype})' if src else f"CAST(NULL AS {dtype})"
        parts.append(f'{expr} AS "{name}"')
        if not src:
            log.warning("Column %s not in file; loading as NULL", name)
    return ", ".join(parts)


def load_month(parquet_path: str | Path, month: str, db_path: str | Path) -> int:
    """Replace the month's partition in raw.yellow_trips; return rows loaded.

    Idempotent: delete-then-insert for ``source_month`` runs in ONE transaction,
    so reruns and retries converge to the same table and a failed insert rolls
    back instead of leaving the month empty.
    """
    month_start, _ = month_bounds(month)
    parquet = str(parquet_path)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        _create_table(con)
        select_list = _select_list(con, parquet)
        con.execute("BEGIN")
        try:
            deleted = con.execute(
                f"DELETE FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE source_month = ?", [month_start]
            ).fetchone()
            con.execute(
                f"INSERT INTO {RAW_SCHEMA}.{RAW_TABLE} "
                f"SELECT {select_list}, ?::DATE, now()::TIMESTAMP FROM read_parquet(?)",
                [month_start, parquet],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        loaded = con.execute(
            f"SELECT count(*) FROM {RAW_SCHEMA}.{RAW_TABLE} WHERE source_month = ?", [month_start]
        ).fetchone()[0]
    finally:
        con.close()
    log.info("raw.yellow_trips %s: replaced %s old rows with %s new rows", month, deleted[0] if deleted else 0, loaded)
    return loaded
