from dateutil import tz
from typing import List, Optional

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.utils.log import get_logger
from elementary.utils.time import convert_utc_iso_format_to_datetime

logger = get_logger(__name__)


class Alert:
    def __init__(
        self,
        id: str,
        detected_at: str = None,
        database_name: str = None,
        schema_name: str = None,
        elementary_database_and_schema: str = None,
        owners: str = None,
        tags: str = None,
        status: str = None,
        subscribers: Optional[List[str]] = None,
        slack_channel: Optional[str] = None,
        timezone: Optional[str] = None,
        meta: Optional[dict] = None,
        **kwargs,
    ):
        self.id = id
        self.elementary_database_and_schema = elementary_database_and_schema
        self.detected_at_utc = None
        self.detected_at = None
        self.timezone = timezone
        try:
            detected_at_datetime = convert_utc_iso_format_to_datetime(detected_at)
            self.detected_at_utc = detected_at_datetime
            self.detected_at = detected_at_datetime.astimezone(
                tz.gettz(timezone) if timezone else tz.tzlocal()
            )
        except Exception:
            logger.error(f'Failed to parse "detected_at" field.')
        self.database_name = database_name
        self.schema_name = schema_name
        self.owners = owners
        self.tags = tags
        self.meta = meta
        self.status = status
        self.subscribers = subscribers
        self.slack_channel = slack_channel

    _LONGEST_MARKDOWN_SUFFIX_LEN = 3
    _CONTINUATION_SYMBOL = "..."

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        raise NotImplementedError
