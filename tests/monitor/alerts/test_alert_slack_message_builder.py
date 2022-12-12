import json

import pytest

from elementary.monitor.alerts.alert import (
    AlertSlackMessageBuilder,
    PreviewIsTooLongError,
)
from elementary.monitor.alerts.schema.slack_alert import (
    AlertDetailsPartSlackMessageSchema,
    AlertSlackMessageSchema,
)


def test_add_title_to_slack_alert():
    message_builder = AlertSlackMessageBuilder()
    title = message_builder.create_header_block("This is an header!")
    sub_title = message_builder.create_context_block(["I am only a sub title :("])
    message_builder._add_title_to_slack_alert([title, sub_title])
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
    message_builder = AlertSlackMessageBuilder()
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
    with pytest.raises(PreviewIsTooLongError) as preview_is_too_long_error:
        validated_preview_blocks = message_builder._validate_preview_blocks(
            [block, block, block, block, block, block]
        )


def test_add_preview_to_slack_alert():
    message_builder = AlertSlackMessageBuilder()
    title = message_builder.create_header_block("This is an header!")
    sub_title = message_builder.create_context_block(["I am only a sub title :("])
    message_builder._add_preview_to_slack_alert([title, sub_title])
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
    block = AlertSlackMessageBuilder.create_divider_block()

    # No details blocks
    message_builder = AlertSlackMessageBuilder()
    details = AlertDetailsPartSlackMessageSchema()
    message_builder._add_details_to_slack_alert(details)
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [{"blocks": []}],
        },
        sort_keys=True,
    )

    # Only result details
    message_builder = AlertSlackMessageBuilder()
    details = AlertDetailsPartSlackMessageSchema(result=[block, block])
    message_builder._add_details_to_slack_alert(details)
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [
                {
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":mag: *Result*",
                            },
                        },
                        {"type": "divider"},
                        block,
                        block,
                    ]
                }
            ],
        },
        sort_keys=True,
    )

    # Only configuration details
    message_builder = AlertSlackMessageBuilder()
    details = AlertDetailsPartSlackMessageSchema(configuration=[block, block])
    message_builder._add_details_to_slack_alert(details)
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [
                {
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":hammer_and_wrench: *Configuration*",
                            },
                        },
                        {"type": "divider"},
                        block,
                        block,
                    ]
                }
            ],
        },
        sort_keys=True,
    )

    # All details
    message_builder = AlertSlackMessageBuilder()
    details = AlertDetailsPartSlackMessageSchema(
        configuration=[block, block], result=[block, block]
    )
    message_builder._add_details_to_slack_alert(details)
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [
                {
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":mag: *Result*",
                            },
                        },
                        {"type": "divider"},
                        block,
                        block,
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": ":hammer_and_wrench: *Configuration*",
                            },
                        },
                        {"type": "divider"},
                        block,
                        block,
                    ]
                }
            ],
        },
        sort_keys=True,
    )
