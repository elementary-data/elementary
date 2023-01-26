import json

import pytest

from elementary.monitor.alerts.alert import (
    PreviewIsTooLongError,
    SlackAlertMessageBuilder,
)


def test_add_title_to_slack_alert():
    message_builder = SlackAlertMessageBuilder()
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
    block = SlackAlertMessageBuilder.create_divider_block()

    # No result and configuration blocks
    message_builder = SlackAlertMessageBuilder()
    message_builder._add_details_to_slack_alert()
    assert json.dumps(message_builder.slack_message, sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [{"blocks": []}],
        },
        sort_keys=True,
    )

    # Only result blocks
    message_builder = SlackAlertMessageBuilder()
    message_builder._add_details_to_slack_alert(result=[block, block])
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

    # Only configuration blocks
    message_builder = SlackAlertMessageBuilder()
    message_builder._add_details_to_slack_alert(configuration=[block, block])
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
    message_builder = SlackAlertMessageBuilder()
    message_builder._add_details_to_slack_alert(
        configuration=[block, block], result=[block, block]
    )
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


def test_prettify_and_dedup_list():
    message_builder = SlackAlertMessageBuilder()
    list_prettified = message_builder.prettify_and_dedup_list(
        ["name1", "name2", "name2"]
    )
    assert list_prettified == "name1, name2" or list_prettified == "name2, name1"

    assert (
        message_builder.prettify_and_dedup_list("name1, name2, name2")
        == "name1, name2, name2"
    )

    string_of_list_prettified = message_builder.prettify_and_dedup_list(
        '["name1", "name2", "name2"]'
    )
    assert (
        string_of_list_prettified == "name1, name2"
        or string_of_list_prettified == "name2, name1"
    )

    assert message_builder.prettify_and_dedup_list({}) == {}
    assert message_builder.prettify_and_dedup_list(123) == 123
