from datetime import datetime
from dateutil import tz


def convert_utc_time_to_local_time(utc_time: 'datetime') -> 'datetime':
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc_time = utc_time.replace(tzinfo=from_zone)
    return utc_time.astimezone(to_zone)
