from datetime import datetime
from dateutil import tz

MILLISECONDS_IN_SEC = 1000
MILLISECONDS_IN_MIN = (1000 * 60)
MILLISECONDS_IN_HOUR = (1000 * 60 * 60)


def convert_utc_time_to_local_time(utc_time: 'datetime') -> 'datetime':
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc_time = utc_time.replace(tzinfo=from_zone)
    return utc_time.astimezone(to_zone)


def format_milliseconds(duration: int) -> str:

    seconds = int((duration / MILLISECONDS_IN_SEC) % 60)
    minutes = int((duration / MILLISECONDS_IN_MIN) % 60)
    hours = int(duration / MILLISECONDS_IN_HOUR)

    remaining_milliseconds = duration - (hours * MILLISECONDS_IN_HOUR + minutes * MILLISECONDS_IN_MIN +
                                         seconds * MILLISECONDS_IN_SEC)

    return f'{hours}h:{minutes}m:{seconds}s:{remaining_milliseconds}ms'