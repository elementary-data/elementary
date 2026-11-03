import os
from pathlib import Path

from elementary.messages.blocks import (
    ActionsBlock,
    CodeBlock,
    LineBlock,
    LinesBlock,
    MentionBlock,
    TableBlock,
    TextBlock,
    UserSelectActionBlock,
)
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

    def test_usergroup_id_formats_as_subteam_mention(self):
        message = MessageBody(
            blocks=[
                LinesBlock(lines=[LineBlock(inlines=[MentionBlock(user="@data")])]),
                ActionsBlock(
                    actions=[
                        UserSelectActionBlock(
                            action_id="assign_user",
                            placeholder="Assign to user",
                            initial_user="@data",
                        )
                    ]
                ),
            ]
        )
        result = format_block_kit(message, resolve_mention=lambda user: "S_DATA")
        assert result.blocks[0]["text"]["text"] == "<!subteam^S_DATA>"
        assert "initial_user" not in result.blocks[1]["elements"][0]

        user_message = MessageBody(
            blocks=[
                ActionsBlock(
                    actions=[
                        UserSelectActionBlock(
                            action_id="assign_user",
                            placeholder="Assign to user",
                            initial_user="@jessica",
                        )
                    ]
                )
            ]
        )
        user_result = format_block_kit(
            user_message, resolve_mention=lambda user: "U_JESSICA"
        )
        assert user_result.blocks[0]["elements"][0]["initial_user"] == "U_JESSICA"

    def test_table_block_none_and_empty_cells_produce_non_empty_text(self):
        table = TableBlock.from_dicts(
            [
                {"col_a": None, "col_b": "value"},
                {"col_a": "", "col_b": "other"},
            ]
        )
        result = format_block_kit(MessageBody(blocks=[table]))
        table_block = result.blocks[0]
        assert table_block["type"] == "table"
        for row in table_block["rows"][1:]:  # skip header row
            for cell in row:
                assert cell["text"], f"raw_text cell must not be empty, got: {cell}"


def _section_texts(result: FormattedBlockKitMessage) -> list:
    return [block["text"]["text"] for block in result.blocks]


def test_long_lines_block_is_split_into_sections_at_line_breaks():
    lines = [f"line {i} " + "x" * 90 for i in range(80)]
    message = MessageBody(
        blocks=[
            LinesBlock(
                lines=[LineBlock(inlines=[TextBlock(text=line)]) for line in lines]
            )
        ]
    )
    texts = _section_texts(format_block_kit(message))
    assert len(texts) == 3
    assert all(len(text) <= 3000 for text in texts)
    assert "\n".join(texts).split("\n") == lines


def test_single_long_line_is_split_at_spaces():
    words = ["word" + str(i).zfill(4) for i in range(700)]
    message = MessageBody(
        blocks=[
            LinesBlock(lines=[LineBlock(inlines=[TextBlock(text=" ".join(words))])])
        ]
    )
    texts = _section_texts(format_block_kit(message))
    assert len(texts) == 3
    assert all(len(text) <= 3000 for text in texts)
    assert " ".join(texts).split(" ") == words


def test_long_code_block_is_split_into_code_sections():
    code_lines = [f"select {i} as col_{i}" + " " * 30 for i in range(150)]
    message = MessageBody(blocks=[CodeBlock(text="\n".join(code_lines))])
    texts = _section_texts(format_block_kit(message))
    assert len(texts) == 3
    assert all(len(text) <= 3000 for text in texts)
    assert all(text.startswith("```") and text.endswith("```") for text in texts)
    assert "\n".join(text[3:-3] for text in texts).split("\n") == code_lines


def test_split_keeps_other_spaces_and_adds_no_empty_section():
    line = "x" * 3000 + " " + "a  b"
    message = MessageBody(
        blocks=[
            LinesBlock(lines=[LineBlock(inlines=[TextBlock(text=line)])]),
            LinesBlock(lines=[LineBlock(inlines=[TextBlock(text="x" * 3000 + " ")])]),
        ]
    )
    texts = _section_texts(format_block_kit(message))
    assert texts == ["x" * 3000, "a  b", "x" * 3000]
