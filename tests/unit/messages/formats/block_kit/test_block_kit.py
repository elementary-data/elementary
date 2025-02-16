import os
from pathlib import Path
from typing import cast

from elementary.messages.formats.block_kit import (
    FormattedBlockKitMessage,
    format_block_kit,
)
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.slack_web import (
    SlackWebMessagingIntegration,
)
from tests.unit.messages.formats.base_test_format import BaseTestFormat
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestBlockKit(BaseTestFormat[FormattedBlockKitMessage]):
    def format(self, message_body: MessageBody) -> FormattedBlockKitMessage:
        return format_block_kit(message_body, resolve_mention=lambda x: "resolved_" + x)

    def get_expected_file_path(self, name: str) -> Path:
        return get_expected_json_path(FIXTURES_DIR, f"{name}.json")

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
        assert_expected_json(cast(dict, result), expected_file_path)
