import json

import pytest

from elementary.monitor.data_monitoring.alerts.integrations.slack.message_builder import (
    PreviewIsTooLongError,
    SlackAlertMessageBuilder,
    SlackAlertMessageSchema,
)


def test_add_title_to_slack_alert():
    message_builder = SlackAlertMessageBuilder()
    title = message_builder.create_header_block("This is an header!")
    sub_title = message_builder.create_context_block(["I am only a sub title :("])
    message_builder.add_title_to_slack_alert([title, sub_title])
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [
                title,
                sub_title,
                {"type": "divider"},
            ],
            "attachments": [{"blocks": []}],
        },
        sort_keys=True,
    )


def test_validate_preview_blocks():
    message_builder = SlackAlertMessageBuilder()
    block = message_builder.create_divider_block()

    # No blocks
    validated_preview_blocks = message_builder._validate_preview_blocks([])
    assert validated_preview_blocks is None

    # Under 5 blocks
    validated_preview_blocks = message_builder._validate_preview_blocks([block, block])
    assert len(validated_preview_blocks) == 5
    assert json.dumps(validated_preview_blocks, sort_keys=True) == json.dumps(
        [
            block,
            block,
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\t",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\t",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\t",
                },
            },
        ],
        sort_keys=True,
    )

    # 5 blocks
    validated_preview_blocks = message_builder._validate_preview_blocks(
        [block, block, block, block, block]
    )
    assert len(validated_preview_blocks) == 5
    assert json.dumps(validated_preview_blocks, sort_keys=True) == json.dumps(
        [
            block,
            block,
            block,
            block,
            block,
        ],
        sort_keys=True,
    )

    # over 5 blocks
    with pytest.raises(PreviewIsTooLongError):
        message_builder._validate_preview_blocks(
            [block, block, block, block, block, block]
        )


def test_add_preview_to_slack_alert():
    message_builder = SlackAlertMessageBuilder()
    title = message_builder.create_header_block("This is an header!")
    sub_title = message_builder.create_context_block(["I am only a sub title :("])
    message_builder.add_preview_to_slack_alert([title, sub_title])
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [
                {
                    "blocks": [
                        title,
                        sub_title,
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "\t",
                            },
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "\t",
                            },
                        },
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "\t",
                            },
                        },
                    ]
                }
            ],
        },
        sort_keys=True,
    )


def test_add_details_to_slack_alert():
    block = SlackAlertMessageBuilder.create_divider_block()

    # No details
    message_builder = SlackAlertMessageBuilder()
    message_builder.add_details_to_slack_alert()
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [{"blocks": []}],
        },
        sort_keys=True,
    )

    # Empty details
    message_builder = SlackAlertMessageBuilder()
    message_builder.add_details_to_slack_alert([])
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [{"blocks": []}],
        },
        sort_keys=True,
    )

    # With details
    message_builder = SlackAlertMessageBuilder()
    message_builder.add_details_to_slack_alert([block, block])
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [
                {
                    "blocks": [
                        block,
                        block,
                    ]
                }
            ],
        },
        sort_keys=True,
    )


def test_full_width_preview_goes_to_blocks_not_attachments():
    """With full_width=True, preview blocks are validated and added to main blocks."""
    message_builder = SlackAlertMessageBuilder(full_width=True)
    block = message_builder.create_header_block("Preview header")
    message_builder.add_preview_to_slack_alert([block])
    assert len(message_builder.slack_message["blocks"]) == 5
    assert message_builder.slack_message["blocks"][0] == block
    assert message_builder.slack_message["attachments"][0]["blocks"] == []


def test_full_width_details_go_to_blocks_not_attachments():
    """With full_width=True, detail blocks are added to main blocks."""
    message_builder = SlackAlertMessageBuilder(full_width=True)
    block = message_builder.create_divider_block()
    message_builder.add_details_to_slack_alert([block])
    assert len(message_builder.slack_message["blocks"]) == 1
    assert message_builder.slack_message["blocks"][0] == block
    assert message_builder.slack_message["attachments"][0]["blocks"] == []


def test_full_width_get_slack_message_structure():
    """With full_width=True, get_slack_message adds rich_text first, title/preview/details in blocks, and clears attachments."""
    message_builder = SlackAlertMessageBuilder(full_width=True)
    title = message_builder.create_header_block("Alert title")
    preview_block = message_builder.create_text_section_block("Preview text")
    detail_block = message_builder.create_divider_block()
    schema = SlackAlertMessageSchema(
        title=[title],
        preview=[preview_block],
        details=[detail_block],
    )
    result = message_builder.get_slack_message(alert_schema=schema)

    blocks = result.blocks
    valid_rich_text_block = {
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_section",
                "elements": [{"type": "text", "text": " "}],
            }
        ],
    }
    assert blocks[0] == valid_rich_text_block
    assert blocks[1] == title
    assert blocks[2]["type"] == "divider"
    assert blocks[3] == preview_block
    # Blocks 4-7 are padding from preview validation
    assert blocks[8] == detail_block
    assert len(blocks) == 9

    assert result.attachments == []
