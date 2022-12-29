from datetime import datetime

import pytest

from elementary.utils.time import (
    convert_local_time_to_timezone,
    convert_utc_time_to_timezone,
)


@pytest.mark.skip(
    reason="There is a different between running the test on GH action and locally"
)
def test_convert_local_time_to_timezone():
    time = datetime.fromisoformat("2022-12-20 11:01:34")
    utc_time = convert_local_time_to_timezone(time)
    est_time = convert_local_time_to_timezone(time, "EST")
    unknown_timezone_time = convert_local_time_to_timezone(time, "Unkown")
    assert utc_time.isoformat() == "2022-12-20T09:01:34+00:00"
    assert est_time.isoformat() == "2022-12-20T04:01:34-05:00"
    # Should be local time
    assert unknown_timezone_time.isoformat() == "2022-12-20T11:01:34+02:00"


@pytest.mark.skip(
    reason="There is a different between running the test on GH action and locally"
)
def test_convert_utc_time_to_timezone():
    utc_time = datetime.fromisoformat("2022-12-20 09:01:34")
    local_time = convert_utc_time_to_timezone(utc_time)
    est_time = convert_utc_time_to_timezone(utc_time, "EST")
    unknown_timezone_time = convert_utc_time_to_timezone(utc_time, "Unkown")
    assert local_time.isoformat() == "2022-12-20T11:01:34+02:00"
    assert est_time.isoformat() == "2022-12-20T04:01:34-05:00"
    # Should be local time
    assert unknown_timezone_time.isoformat() == "2022-12-20T11:01:34+02:00"
