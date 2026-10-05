"""Month helpers shared by download, load and quality checks."""

from __future__ import annotations

import re
from datetime import date

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def validate_month(month: str) -> str:
    """Return ``month`` unchanged if it looks like ``YYYY-MM``, else raise."""
    if not _MONTH_RE.match(month):
        raise ValueError(f"month must be formatted YYYY-MM, got {month!r}")
    return month


def month_bounds(month: str) -> tuple[date, date]:
    """First day of the month and first day of the next month (half-open range)."""
    validate_month(month)
    year, mon = int(month[:4]), int(month[5:])
    start = date(year, mon, 1)
    end = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)
    return start, end
