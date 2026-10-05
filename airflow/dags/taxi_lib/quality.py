"""Post-build data quality checks with readable logs.

dbt tests guard the *models*; these checks guard the *pipeline run*: did the
month land, is it complete enough, and did the transformations keep most rows?
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from taxi_lib.months import month_bounds

log = logging.getLogger(__name__)

# Columns that every real trip must have. Other columns (passenger_count,
# congestion_surcharge, ...) are legitimately null for ~5% of rows, so they are
# reported but never fail the run.
CRITICAL_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]
INFO_COLUMNS = ["passenger_count", "RatecodeID", "congestion_surcharge", "Airport_fee"]


class DataQualityError(AssertionError):
    """Raised when one or more checks fail; the message lists every failure."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def check_quality(
    db_path: str | Path,
    month: str,
    *,
    min_rows: int = 1_000,
    max_critical_null_rate: float = 0.01,
    min_retention: float = 0.85,
) -> list[CheckResult]:
    """Run all checks for ``month``; log each result; raise if any failed."""
    month_start, month_end = month_bounds(month)
    results: list[CheckResult] = []

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        raw_rows = con.execute(
            "SELECT count(*) FROM raw.yellow_trips WHERE source_month = ?", [month_start]
        ).fetchone()[0]
        results.append(
            CheckResult("raw_row_count", raw_rows >= min_rows, f"{raw_rows:,} rows (minimum {min_rows:,})")
        )

        if raw_rows:
            for col in CRITICAL_COLUMNS + INFO_COLUMNS:
                nulls = con.execute(
                    f'SELECT count(*) FILTER (WHERE "{col}" IS NULL) FROM raw.yellow_trips WHERE source_month = ?',
                    [month_start],
                ).fetchone()[0]
                rate = nulls / raw_rows
                if col in CRITICAL_COLUMNS:
                    results.append(
                        CheckResult(
                            f"null_rate[{col}]",
                            rate <= max_critical_null_rate,
                            f"{rate:.4%} null (limit {max_critical_null_rate:.2%})",
                        )
                    )
                else:
                    log.info("INFO null_rate[%s] = %.2f%% (informational, not enforced)", col, rate * 100)

        fct_rows = con.execute(
            "SELECT count(*) FROM marts.fct_trips WHERE pickup_date >= ? AND pickup_date < ?",
            [month_start, month_end],
        ).fetchone()[0]
        retention = fct_rows / raw_rows if raw_rows else 0.0
        results.append(
            CheckResult(
                "fct_trips_retention",
                fct_rows > 0 and retention >= min_retention,
                f"{fct_rows:,} of {raw_rows:,} raw rows survived cleaning ({retention:.2%}, minimum {min_retention:.0%})",
            )
        )
    finally:
        con.close()

    for r in results:
        (log.info if r.passed else log.error)("%s %s: %s", "PASS" if r.passed else "FAIL", r.name, r.detail)

    failures = [r for r in results if not r.passed]
    if failures:
        raise DataQualityError(
            f"{len(failures)} data quality check(s) failed for {month}: "
            + "; ".join(f"{r.name} -> {r.detail}" for r in failures)
        )
    log.info("All %s data quality checks passed for %s", len(results), month)
    return results
