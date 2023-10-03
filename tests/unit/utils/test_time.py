from datetime import datetime

from dateutil import tz

from elementary.utils.time import datetime_strftime, get_formatted_timedelta


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
