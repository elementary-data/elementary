from typing import List, Union

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.schema import SlackBlocksType, SlackMessageSchema
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
)
from elementary.utils.json_utils import unpack_and_flatten_str_to_list


class SlackMessageBuilder:
    _LONGEST_MARKDOWN_SUFFIX_LEN = 3
    _CONTINUATION_SYMBOL = "..."
    _MAX_SLACK_SECTION_SIZE = 2
    _MAX_ALERT_PREVIEW_BLOCKS = 5
    _MAX_AMOUNT_OF_ATTACHMENTS = 50
    _HASHTAG = "#"

    def __init__(self) -> None:
        self.slack_message = self._initial_slack_message()

    @classmethod
    def _initial_slack_message(cls) -> dict:
        return {"blocks": [], "attachments": [{"blocks": []}]}

    def reset_slack_message(self):
        self.slack_message = self._initial_slack_message()

    def _add_always_displayed_blocks(self, blocks: SlackBlocksType):
        # In oppose to attachments Blocks are always displayed, use this for parts that should always be displayed in the message
        self.slack_message["blocks"].extend(blocks)

    def _add_blocks_as_attachments(self, blocks: SlackBlocksType):
        # The first 5 attachments Blocks are always displayed.
        # The rest of the attachments Blocks are hidden behind "show more" button.
        # NOTICE: attachments blocks are deprecated by Slack.
        self.slack_message["attachments"][0]["blocks"].extend(blocks)

    @staticmethod
    def get_limited_markdown_msg(section_msg: str) -> str:
        if len(section_msg) < SectionBlock.text_max_length:
            return section_msg
        return (
            section_msg[
                : SectionBlock.text_max_length
                - len(SlackMessageBuilder._CONTINUATION_SYMBOL)
                - SlackMessageBuilder._LONGEST_MARKDOWN_SUFFIX_LEN
            ]
            + SlackMessageBuilder._CONTINUATION_SYMBOL
            + section_msg[-SlackMessageBuilder._LONGEST_MARKDOWN_SUFFIX_LEN :]
        )

    @staticmethod
    def create_divider_block() -> dict:
        return {"type": "divider"}

    @staticmethod
    def create_fields_section_block(section_msgs: list) -> dict:
        fields = []
        for section_msg in section_msgs:
            fields.append(
                {
                    "type": "mrkdwn",
                    "text": SlackMessageBuilder.get_limited_markdown_msg(section_msg),
                }
            )

        return {"type": "section", "fields": fields}

    @staticmethod
    def create_text_section_block(section_msg: str) -> dict:
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": SlackMessageBuilder.get_limited_markdown_msg(section_msg),
            },
        }

    @staticmethod
    def create_empty_section_block() -> dict:
        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": SlackMessageBuilder.get_limited_markdown_msg("\t"),
            },
        }

    @staticmethod
    def create_context_block(context_msgs: list) -> dict:
        fields = []
        for context_msg in context_msgs:
            fields.append(
                {
                    "type": "mrkdwn",
                    "text": SlackMessageBuilder.get_limited_markdown_msg(context_msg),
                }
            )

        return {"type": "context", "elements": fields}

    @staticmethod
    def create_section_with_button(text: str, url: ReportLinkData) -> dict:
        return {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text},
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": url.text, "emoji": True},
                "url": url.url,
            },
        }

    @staticmethod
    def create_header_block(msg: str) -> dict:
        return {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": msg,
            },
        }

    @staticmethod
    def create_button_action_block(text: str, url: str) -> dict:
        return {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": text, "emoji": True},
                    "value": text,
                    "url": url,
                }
            ],
        }

    @staticmethod
    def create_compacted_sections_blocks(section_msgs: list) -> List[dict]:
        # Compacting sections into attachments.
        # Each section can contain _MAX_SLACK_SECTION_SIZE fields.
        attachments = []
        section_fields: List[dict] = []

        for section_msg in section_msgs:
            section_field = {
                "type": "mrkdwn",
                "text": SlackMessageBuilder.get_limited_markdown_msg(section_msg),
            }
            if len(section_fields) < SlackMessageBuilder._MAX_SLACK_SECTION_SIZE:
                section_fields.append(section_field)
            else:
                attachment = {"type": "section", "fields": section_fields}
                attachments.append(attachment)
                section_fields = [section_field]

        attachment = {"type": "section", "fields": section_fields}
        attachments.append(attachment)
        return attachments

    def get_slack_message(self, *args, **kwargs) -> SlackMessageSchema:
        return SlackMessageSchema(**self.slack_message)

    @staticmethod
    def prettify_and_dedup_list(str_list: Union[List[str], str]) -> str:
        """
        Receives a list of strings, either JSON dumped or not, dedups and sorts it, and returns it as a comma-separated
        string.
        This is useful for various lists we include in Slack messages (owners, subscribers, etc.)
        """
        if isinstance(str_list, str):
            str_list = unpack_and_flatten_str_to_list(str_list)
        return ", ".join(sorted(set(str_list)))
