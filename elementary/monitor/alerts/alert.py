from datetime import datetime
from dateutil import tz
from typing import List, Optional

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.utils.log import get_logger
from elementary.utils.time import convert_utc_iso_format_to_datetime

logger = get_logger(__name__)

MAX_SLACK_SECTION_SIZE = 2
SHOW_MORE_ATTACHMENTS_MARK = 5


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

    @classmethod
    def _initial_slack_message(cls):
        return {"blocks": [], "attachments": [{"blocks": []}]}

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
    def _add_divider(cls, slack_message: dict, add_to_attachment: bool = False):
        if add_to_attachment:
            slack_message["attachments"][0]["blocks"].extend([{"type": "divider"}])
        else:
            slack_message["blocks"].extend([{"type": "divider"}])

    @classmethod
    def _add_fields_section_to_slack_msg(
        cls, slack_message: dict, section_msgs: list, add_to_attachment: bool = False
    ):
        fields = []
        for section_msg in section_msgs:
            fields.append(
                {"type": "mrkdwn", "text": cls._format_section_msg(section_msg)}
            )

        block = [{"type": "section", "fields": fields}]

        if add_to_attachment:
            slack_message["attachments"][0]["blocks"].extend(block)
        else:
            slack_message["blocks"].extend(block)

    @classmethod
    def _add_text_section_to_slack_msg(
        cls, slack_message: dict, section_msg: str, add_to_attachment: bool = False
    ):
        block = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": cls._format_section_msg(section_msg),
                },
            }
        ]
        if add_to_attachment:
            slack_message["attachments"][0]["blocks"].extend(block)
        else:
            slack_message["blocks"].extend(block)

    @classmethod
    def _add_empty_section_to_slack_msg(
        cls, slack_message: dict, add_to_attachment: bool = False
    ):
        block = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": cls._format_section_msg("\t"),
                },
            }
        ]
        if add_to_attachment:
            slack_message["attachments"][0]["blocks"].extend(block)
        else:
            slack_message["blocks"].extend(block)

    @classmethod
    def _add_context_to_slack_msg(
        cls, slack_message: dict, context_msgs: list, add_to_attachment: bool = False
    ):
        fields = []
        for context_msg in context_msgs:
            fields.append(
                {
                    "type": "mrkdwn",
                    "text": cls._format_section_msg(context_msg),
                }
            )

        block = [{"type": "context", "elements": fields}]
        if add_to_attachment:
            slack_message["attachments"][0]["blocks"].extend(block)
        else:
            slack_message["blocks"].extend(block)

    @classmethod
    def _add_header_to_slack_msg(
        cls, slack_message: dict, msg: str, add_to_attachment: bool = False
    ):
        block = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": msg,
                },
            }
        ]
        if add_to_attachment:
            slack_message["attachments"][0]["blocks"].extend(block)
        else:
            slack_message["blocks"].extend(block)

    @classmethod
    def _add_compacted_sections_to_slack_msg(
        cls,
        slack_message: dict,
        section_msgs: str,
        add_to_attachment: bool = False,
        pad_to_show_more_mark: bool = False,
    ):
        # Compacting sections into attachments.
        # Each section can contian MAX_SLACK_SECTION_SIZE fields.
        attachments = []
        section_fields = []

        for section_msg in section_msgs:
            section_field = {
                "type": "mrkdwn",
                "text": cls._format_section_msg(section_msg),
            }
            if len(section_fields) < MAX_SLACK_SECTION_SIZE:
                section_fields.append(section_field)
            else:
                attachment = {"type": "section", "fields": section_fields}
                attachments.append(attachment)
                section_fields = [section_field]

        attachment = {"type": "section", "fields": section_fields}
        attachments.append(attachment)

        while pad_to_show_more_mark and len(attachments) < SHOW_MORE_ATTACHMENTS_MARK:
            section_field = {"type": "mrkdwn", "text": "\t"}
            attachment = {"type": "section", "fields": [section_field]}
            attachments.append(attachment)

        if add_to_attachment:
            slack_message["attachments"][0]["blocks"].extend(attachments)
        else:
            slack_message["blocks"].extend(attachments)

    def _get_slack_status_icon(self) -> str:
        icon = ":small_red_triangle:"
        if self.status == "warn":
            icon = ":warning:"
        elif self.status == "error":
            icon = ":x:"
        return icon
