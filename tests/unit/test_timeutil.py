"""Tests for generation/timeutil.py: naive-datetime <-> epoch-seconds conversion.

Every timestamp in generation/ is a naive datetime, and this module is the
only place that converts to/from epoch seconds; it deliberately avoids
datetime.timestamp() because that silently applies the host's local
timezone. These tests check the round trip, the fixed epoch, and boundary
cases (leap day, month/year rollover, sub-second truncation).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from generation.timeutil import EPOCH, from_epoch_s, to_epoch_s


def test_epoch_itself_maps_to_zero() -> None:
    assert to_epoch_s(EPOCH) == 0
    assert from_epoch_s(0) == EPOCH


@pytest.mark.parametrize(
    "moment",
    [
        datetime(1970, 1, 1),
        datetime(2023, 1, 1),
        datetime(2026, 8, 4, 12, 30, 45),
        datetime(2024, 2, 29, 23, 59, 59),  # leap day
        datetime(2000, 2, 29),  # century leap year
        datetime(2027, 12, 31, 23, 59, 59),  # DATE_DIM_UPPER_BOUND
    ],
)
def test_round_trip_through_epoch_seconds(moment: datetime) -> None:
    assert from_epoch_s(to_epoch_s(moment)) == moment


def test_to_epoch_s_truncates_sub_second_precision() -> None:
    """to_epoch_s returns an int; microseconds are truncated toward zero,
    matching int()'s behavior on a float total_seconds()."""
    moment = datetime(2023, 6, 15, 12, 0, 0, 500_000)
    assert to_epoch_s(moment) == to_epoch_s(datetime(2023, 6, 15, 12, 0, 0))


def test_to_epoch_s_handles_a_full_year_including_a_leap_year() -> None:
    """One non-leap year is 365 * 86400 seconds; crossing Feb 29 in a leap
    year adds the extra day, both computed correctly by plain subtraction
    from the fixed epoch rather than any calendar-aware library call."""
    start = datetime(2023, 1, 1)
    end_non_leap = datetime(2024, 1, 1)
    assert to_epoch_s(end_non_leap) - to_epoch_s(start) == 365 * 86400

    start_leap = datetime(2024, 1, 1)
    end_leap = datetime(2025, 1, 1)
    assert to_epoch_s(end_leap) - to_epoch_s(start_leap) == 366 * 86400


def test_month_boundary_crossing_is_exact() -> None:
    """January has 31 days; the epoch-second delta across that boundary
    must match exactly, with no drift from any implicit local-timezone
    conversion."""
    jan_31 = datetime(2023, 1, 31, 23, 0, 0)
    feb_1 = datetime(2023, 2, 1, 1, 0, 0)
    assert to_epoch_s(feb_1) - to_epoch_s(jan_31) == 2 * 3600


def test_from_epoch_s_accepts_float_seconds() -> None:
    result = from_epoch_s(3600.0)
    assert result == EPOCH + timedelta(hours=1)


def test_to_epoch_s_before_epoch_is_negative() -> None:
    """Naive datetimes are accepted unconditionally, even before 1970;
    nothing in this module special-cases the sign."""
    moment = datetime(1969, 12, 31, 23, 0, 0)
    assert to_epoch_s(moment) == -3600


def test_from_epoch_s_of_negative_seconds_round_trips() -> None:
    assert to_epoch_s(from_epoch_s(-3600)) == -3600
