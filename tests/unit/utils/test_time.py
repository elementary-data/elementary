import re
from datetime import datetime
from unittest.mock import patch

import pytest
from dateutil import tz

from elementary.utils.time import (
    convert_partial_iso_format_to_full_iso_format,
    convert_time_to_timezone,
    datetime_strftime,
    get_formatted_timedelta,
)


def test_datetime_strftime_without_timezone():
    str_datetime = datetime_strftime(
        datetime(2010, 1, 1, 1, 1, 1, 1), include_timezone=False
    )
    assert str_datetime == "2010-01-01 01:01:01"


def test_datetime_strftime_with_timezone():
    str_datetime = datetime_strftime(
        datetime(2010, 1, 1, 1, 1, 1, 1, tzinfo=tz.tzutc()), include_timezone=True
    )
    assert str_datetime == "2010-01-01 01:01:01 UTC"


def test_get_formatted_timedelta_days_delta():
    formatted_timedelta = get_formatted_timedelta(173000)
    assert formatted_timedelta == "2 days 0h 3m 20s"


def test_get_formatted_timedelta_hours_delta():
    formatted_timedelta = get_formatted_timedelta(43200)
    assert formatted_timedelta == "12 hours 0m 0s"


def test_get_formatted_timedelta_minutes_delta():
    formatted_timedelta = get_formatted_timedelta(3001)
    assert formatted_timedelta == "50 minutes 1s"


def test_get_formatted_timedelta_seconds_delta():
    formatted_timedelta = get_formatted_timedelta(12)
    assert formatted_timedelta == "12 seconds"


def test_convert_time_to_timezone():
    # test time with no timezone is handled by default as utc
    date = datetime(2010, 1, 1, 1, 1, 1, 1)
    date_with_timezone = convert_time_to_timezone(date)
    assert date.tzname() is None
    assert date_with_timezone.tzname() == "UTC"

    # test default timezone to convert to is utc by default
    date = datetime.fromisoformat("2010-01-01T01:01:01+01:00")
    date_with_timezone = convert_time_to_timezone(date)
    assert date.tzname() == "UTC+01:00"
    assert date_with_timezone.tzname() == "UTC"
    assert date.hour == 1
    assert date_with_timezone.hour == 0
    assert (date - date_with_timezone).total_seconds() == 0

    date = datetime.fromisoformat("2010-01-01T01:01:01+01:00")
    utc_plus_2_timezone = "Etc/GMT-2"
    date_with_timezone = convert_time_to_timezone(date, utc_plus_2_timezone)
    assert date.tzname() == "UTC+01:00"
    assert date_with_timezone.tzname() == "+02"
    assert date.hour == 1
    assert date_with_timezone.hour == 2
    assert (date - date_with_timezone).total_seconds() == 0


@pytest.mark.parametrize(
    "input_time, expected_output",
    [
        pytest.param(
            "2024-01-01T12:00:00+00",
            "2024-01-01T12:00:00+00:00",
            id="abbreviated_utc_offset",
        ),
        pytest.param(
            "2024-01-01T12:00:00-05",
            "2024-01-01T12:00:00-05:00",
            id="abbreviated_negative_offset",
        ),
        pytest.param(
            "2024-01-01 12:00:00.123456+00",
            "2024-01-01T12:00:00+00:00",
            id="abbreviated_offset_with_fractional_seconds",
        ),
        pytest.param(
            "2024-01-01T12:00:00+00:00",
            "2024-01-01T12:00:00+00:00",
            id="full_utc_offset",
        ),
        pytest.param(
            "2024-01-01T12:00:00+05:30",
            "2024-01-01T12:00:00+05:30",
            id="full_non_utc_offset",
        ),
        pytest.param(
            "2024-01-01T12:00:00",
            "2024-01-01T12:00:00+00:00",
            id="no_offset_defaults_to_utc",
        ),
        pytest.param(
            "2024-01-15",
            "2024-01-15T00:00:00+00:00",
            id="date_only_not_corrupted",
        ),
    ],
)
def test_convert_partial_iso_format_to_full_iso_format(
    input_time: str, expected_output: str
) -> None:
    assert convert_partial_iso_format_to_full_iso_format(input_time) == expected_output


class _StrictLegacyDatetime(datetime):
    """
    A ``datetime`` subclass whose ``fromisoformat`` reproduces CPython's
    pre-3.11 parser: it only accepts a fractional-seconds component of
    exactly 0, 3 or 6 digits (the only widths ``datetime.isoformat()``
    itself ever produces) and raises ``ValueError`` for anything else, such
    as the 9-digit/nanosecond-precision timestamps reported in
    https://github.com/elementary-data/elementary/issues/2221.

    Python 3.11 relaxed ``fromisoformat`` to tolerate (and truncate) any
    number of fractional digits, so the very same input that fails on 3.9/3.10
    parses silently on 3.11+. That made the bug look "platform dependent"
    (it showed up in a Docker image pinned to an older Python while the
    reporter's local machine happened to run a newer one) when it is really a
    Python-version-dependent parsing difference. Subclassing here lets the
    test force that legacy behavior deterministically, regardless of which
    Python actually runs the test suite.
    """

    @classmethod
    def fromisoformat(cls, date_string):
        match = re.search(r"\.(\d+)", date_string)
        if match and len(match.group(1)) not in (3, 6):
            raise ValueError(f"Invalid isoformat string: {date_string!r}")
        return datetime.fromisoformat(date_string)


@pytest.mark.parametrize(
    "input_time, expected_output",
    [
        pytest.param(
            "2026-04-27T14:34:33.609083964Z",
            "2026-04-27T14:34:33+00:00",
            id="nanosecond_precision_utc_z_suffix",
        ),
        pytest.param(
            "2026-04-27T14:34:33.609083964+00:00",
            "2026-04-27T14:34:33+00:00",
            id="nanosecond_precision_full_offset",
        ),
        pytest.param(
            "2026-04-27T14:34:33.6+00:00",
            "2026-04-27T14:34:33+00:00",
            id="single_digit_fraction",
        ),
        pytest.param(
            "2026-04-27T14:34:33.60908+00:00",
            "2026-04-27T14:34:33+00:00",
            id="five_digit_fraction",
        ),
    ],
)
def test_convert_partial_iso_format_to_full_iso_format_handles_any_fractional_precision(
    input_time: str, expected_output: str
) -> None:
    """
    Reproduces the bug from issue #2221: a timestamp whose fractional-seconds
    component isn't exactly 3 or 6 digits (e.g. the nanosecond-precision
    values some environments emit) must still normalize correctly, instead of
    depending on the interpreter's ``datetime.fromisoformat`` leniency. We
    patch the module's ``datetime`` with a subclass that enforces the strict,
    pre-3.11 CPython behavior so the test's outcome does not depend on which
    Python version happens to run it.
    """
    with patch("elementary.utils.time.datetime", _StrictLegacyDatetime):
        assert (
            convert_partial_iso_format_to_full_iso_format(input_time) == expected_output
        )
