import os
from pathlib import Path

from elementary.messages.blocks import TableBlock
from elementary.messages.formats.block_kit import (
    FormattedBlockKitMessage,
    format_block_kit,
)
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.slack_web import (
    SlackWebMessagingIntegration,
)
from tests.unit.messages.formats.base_test_format import BaseTestFormat
from tests.unit.messages.utils import assert_expected_json, get_expected_file_path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestBlockKit(BaseTestFormat[FormattedBlockKitMessage]):
    def format(self, message_body: MessageBody) -> FormattedBlockKitMessage:
        return format_block_kit(message_body, resolve_mention=lambda x: "resolved_" + x)

    def get_expected_file_path(self, name: str) -> Path:
        return get_expected_file_path(FIXTURES_DIR, f"{name}.json")

    def assert_expected_value(
        self, result: FormattedBlockKitMessage, expected_file_path: Path
    ) -> None:
        if "TEST_SLACK_TOKEN" in os.environ and "TEST_SLACK_CHANNEL" in os.environ:
            """
            For testing purposes, add the ability to send messages to Slack channels to see how the message looks.
            """
            messaging_integration = SlackWebMessagingIntegration.from_token(
                token=os.environ["TEST_SLACK_TOKEN"],
            )
            messaging_integration._send_message(
                os.environ["TEST_SLACK_CHANNEL"], result
            )
        assert_expected_json(result.dict(), expected_file_path)

    def test_table_block_none_and_empty_cells_produce_non_empty_text(self):
        table = TableBlock.from_dicts([
            {"col_a": None, "col_b": "value"},
            {"col_a": "",   "col_b": "other"},
        ])
        result = format_block_kit(MessageBody(blocks=[table]))
        table_block = result.blocks[0]
        assert table_block["type"] == "table"
        for row in table_block["rows"][1:]:  # skip header row
            for cell in row:
                assert cell["text"], f"raw_text cell must not be empty, got: {cell}"
