from datetime import datetime, timedelta, timezone
from typing import Optional

from dateutil import tz

from elementary.utils.log import get_logger
from elementary.utils.strings import pluralize_string

logger = get_logger(__name__)

MILLISECONDS_IN_SEC = 1000
MILLISECONDS_IN_MIN = 1000 * 60
MILLISECONDS_IN_HOUR = 1000 * 60 * 60

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATETIME_WITH_TIMEZONE_FORMAT = "%Y-%m-%d %H:%M:%S %Z"


def convert_utc_iso_format_to_datetime(utc_iso_format: str) -> datetime:
    return datetime.fromisoformat(utc_iso_format).replace(tzinfo=tz.tzutc())


def convert_utc_time_to_timezone(
    utc_time: datetime, timezone: Optional[str] = None
) -> datetime:
    from_zone = tz.tzutc()
    to_zone = tz.gettz(timezone) if timezone else tz.tzlocal()
    utc_time_with_timezone = utc_time.replace(tzinfo=from_zone)
    return utc_time_with_timezone.astimezone(to_zone)


def convert_local_time_to_timezone(
    local_time: datetime, timezone: Optional[str] = None
) -> datetime:
    from_zone = tz.tzlocal()
    to_zone = tz.gettz(timezone) if timezone else tz.tzutc()
    local_time_with_timezone = local_time.replace(tzinfo=from_zone)
    return local_time_with_timezone.astimezone(to_zone)


def convert_time_to_timezone(
    time: datetime, timezone: Optional[str] = None
) -> datetime:
    # Converting a datetime to timezone
    # If "time" has no timezone, the default is set to utc.
    # If not "timezone" has provided, the default is set to utc.
    time_timezone = tz.gettz(time.tzname()) if time.tzname() else tz.tzutc()
    to_timezone = tz.gettz(timezone) if timezone else tz.tzutc()
    time_with_timezone = time.replace(tzinfo=time_timezone)
    return time_with_timezone.astimezone(to_timezone)


def get_now_utc_str(format: str = DATETIME_FORMAT) -> str:
    return datetime.utcnow().strftime(format)


def get_now_utc_iso_format() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def format_milliseconds(duration: int) -> str:
    seconds = int((duration / MILLISECONDS_IN_SEC) % 60)
    minutes = int((duration / MILLISECONDS_IN_MIN) % 60)
    hours = int(duration / MILLISECONDS_IN_HOUR)

    remaining_milliseconds = duration - (
        hours * MILLISECONDS_IN_HOUR
        + minutes * MILLISECONDS_IN_MIN
        + seconds * MILLISECONDS_IN_SEC
    )

    return f"{hours}h:{minutes}m:{seconds}s:{remaining_milliseconds}ms"


def convert_datetime_utc_str_to_timezone_str(
    isoformat_datetime: str, timezone: Optional[str], include_timezone: bool = False
) -> str:
    try:
        parsed_time = datetime.fromisoformat(isoformat_datetime)
        datetime_with_timezone = convert_utc_time_to_timezone(parsed_time, timezone)
        return datetime_strftime(datetime_with_timezone, include_timezone)
    except Exception:
        return isoformat_datetime


def datetime_strftime(datetime: datetime, include_timezone: bool = False) -> str:
    return datetime.strftime(
        DATETIME_FORMAT if not include_timezone else DATETIME_WITH_TIMEZONE_FORMAT
    )


def convert_partial_iso_format_to_full_iso_format(partial_iso_format_time: str) -> str:
    try:
        date = datetime.fromisoformat(partial_iso_format_time)
        # Get the given date timezone
        time_zone_name = date.strftime("%Z")
        time_zone = tz.gettz(time_zone_name) if time_zone_name else tz.UTC
        date_with_timezone = date.replace(tzinfo=time_zone, microsecond=0)
        return date_with_timezone.isoformat()
    except ValueError:
        logger.exception(
            f'Failed to covert time string: "{partial_iso_format_time}" to ISO format'
        )
        return partial_iso_format_time


def get_formatted_timedelta(time_ago_in_s: float) -> str:
    delta = timedelta(seconds=time_ago_in_s)
    days = delta.days
    seconds = delta.seconds

    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days > 0:
        duration_sentence = (
            f"{pluralize_string(days, 'day', 'days')} {hours}h {minutes}m {seconds}s"
        )
    else:
        if hours > 0:
            duration_sentence = (
                f"{pluralize_string(hours, 'hour', 'hours')} {minutes}m {seconds}s"
            )
        else:
            if minutes > 0:
                duration_sentence = (
                    f"{pluralize_string(minutes, 'minute', 'minutes')} {seconds}s"
                )
            else:
                duration_sentence = f"{pluralize_string(seconds, 'second', 'seconds')}"

    return duration_sentence
