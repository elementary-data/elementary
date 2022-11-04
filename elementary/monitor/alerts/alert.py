from datetime import datetime
from dateutil import tz
from typing import List, Optional

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.utils.json_utils import prettify_json_str_set
from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT

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
        **kwargs,
    ):
        self.id = id
        self.elementary_database_and_schema = elementary_database_and_schema
        self.detected_at_utc = None
        self.detected_at = None
        self.timezone = timezone
        try:
            detected_at_datetime = datetime.fromisoformat(detected_at)
            self.detected_at = detected_at_datetime.astimezone(tz.tzlocal())
            self.detected_at_utc = detected_at_datetime.astimezone(tz.UTC)
        except Exception:
            logger.error(f'Failed to parse "detected_at" field.')
        self.database_name = database_name
        self.schema_name = schema_name
        self.owners = prettify_json_str_set(owners)
        self.tags = prettify_json_str_set(tags)
        self.status = status
        self.subscribers = subscribers
        self.slack_channel = slack_channel

    _LONGEST_MARKDOWN_SUFFIX_LEN = 3
    _CONTINUATION_SYMBOL = "..."

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        raise NotImplementedError

    @classmethod
    def _format_section_msg(cls, section_msg):
        if len(section_msg) < SectionBlock.text_max_length:
            return section_msg
        return (
            section_msg[
                : SectionBlock.text_max_length
                - len(cls._CONTINUATION_SYMBOL)
                - cls._LONGEST_MARKDOWN_SUFFIX_LEN
            ]
            + cls._CONTINUATION_SYMBOL
            + section_msg[-cls._LONGEST_MARKDOWN_SUFFIX_LEN :]
        )

    @classmethod
    def _add_fields_section_to_slack_msg(
        cls, slack_message: dict, section_msgs: list, divider: bool = False
    ):
        fields = []
        for section_msg in section_msgs:
            fields.append(
                {"type": "mrkdwn", "text": cls._format_section_msg(section_msg)}
            )

        block = []
        if divider:
            block.append({"type": "divider"})
        block.append({"type": "section", "fields": fields})
        slack_message["attachments"][0]["blocks"].extend(block)

    @classmethod
    def _add_text_section_to_slack_msg(
        cls, slack_message: dict, section_msg: str, divider: bool = False
    ):
        block = []
        if divider:
            block.append({"type": "divider"})
        block.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": cls._format_section_msg(section_msg),
                },
            }
        )
        slack_message["attachments"][0]["blocks"].extend(block)
