from datetime import date

import pytest

from taxi_lib.months import month_bounds, validate_month


def test_month_bounds_regular_month():
    assert month_bounds("2025-02") == (date(2025, 2, 1), date(2025, 3, 1))


def test_month_bounds_december_rolls_the_year():
    assert month_bounds("2025-12") == (date(2025, 12, 1), date(2026, 1, 1))


@pytest.mark.parametrize("bad", ["2025-13", "2025-1", "25-01", "2025/01", "2025-01-01", "", "abcd-ef"])
def test_validate_month_rejects_bad_values(bad):
    with pytest.raises(ValueError, match="YYYY-MM"):
        validate_month(bad)
