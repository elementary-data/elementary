import json

import pytest
from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.clients.slack.slack_message_builder import SlackMessageBuilder


def test_create_divider_block():
    divider_block = SlackMessageBuilder.create_divider_block()
    assert json.dumps(divider_block, sort_keys=True) == json.dumps(
        {"type": "divider"}, sort_keys=True
    )


def test_create_fields_section_block():
    section_messages = ["first section"]
    fields_section_block = SlackMessageBuilder.create_fields_section_block(
        section_messages
    )
    assert json.dumps(fields_section_block, sort_keys=True) == json.dumps(
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "first section",
                }
            ],
        },
        sort_keys=True,
    )

    section_messages = ["first section", "second section"]
    fields_section_block = SlackMessageBuilder.create_fields_section_block(
        section_messages
    )
    assert json.dumps(fields_section_block, sort_keys=True) == json.dumps(
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": "first section",
                },
                {
                    "type": "mrkdwn",
                    "text": "second section",
                },
            ],
        },
        sort_keys=True,
    )


@pytest.mark.parametrize(
    "section_message",
    [
        "first",
        "second",
        "third",
    ],
)
def test_create_text_section_block(section_message):
    text_section_block = SlackMessageBuilder.create_text_section_block(section_message)
    assert json.dumps(text_section_block, sort_keys=True) == json.dumps(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": section_message,
            },
        },
        sort_keys=True,
    )


def test_create_empty_section_block():
    empty_section_block = SlackMessageBuilder.create_empty_section_block()
    assert json.dumps(empty_section_block, sort_keys=True) == json.dumps(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\t",
            },
        },
        sort_keys=True,
    )


def test_create_context_block():
    context_messages = ["first section"]
    context_section_block = SlackMessageBuilder.create_context_block(context_messages)
    assert json.dumps(context_section_block, sort_keys=True) == json.dumps(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "first section",
                }
            ],
        },
        sort_keys=True,
    )

    context_messages = ["first section", "second section"]
    context_section_block = SlackMessageBuilder.create_context_block(context_messages)
    assert json.dumps(context_section_block, sort_keys=True) == json.dumps(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "first section",
                },
                {
                    "type": "mrkdwn",
                    "text": "second section",
                },
            ],
        },
        sort_keys=True,
    )


@pytest.mark.parametrize(
    "message",
    [
        "first",
        "second",
        "third",
    ],
)
def test_create_header_block(message):
    header_block = SlackMessageBuilder.create_header_block(message)
    assert json.dumps(header_block, sort_keys=True) == json.dumps(
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": message,
            },
        },
        sort_keys=True,
    )


def test_create_compacted_sections_blocks():
    # no sections
    section_messages = []
    compacted_section = SlackMessageBuilder.create_compacted_sections_blocks(
        section_messages
    )
    assert json.dumps(compacted_section, sort_keys=True) == json.dumps(
        [{"type": "section", "fields": []}], sort_keys=True
    )

    # even sections
    section_messages = ["One", "Two", "Three", "Four"]
    compacted_section = SlackMessageBuilder.create_compacted_sections_blocks(
        section_messages
    )
    assert json.dumps(compacted_section, sort_keys=True) == json.dumps(
        [
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "One"},
                    {"type": "mrkdwn", "text": "Two"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "Three"},
                    {"type": "mrkdwn", "text": "Four"},
                ],
            },
        ],
        sort_keys=True,
    )

    # odd sections
    section_messages = ["One", "Two", "Three", "Four", "Five"]
    compacted_section = SlackMessageBuilder.create_compacted_sections_blocks(
        section_messages
    )
    assert json.dumps(compacted_section, sort_keys=True) == json.dumps(
        [
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "One"},
                    {"type": "mrkdwn", "text": "Two"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "Three"},
                    {"type": "mrkdwn", "text": "Four"},
                ],
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "Five"},
                ],
            },
        ],
        sort_keys=True,
    )


def test_get_slack_status_icon():
    assert SlackMessageBuilder.get_slack_status_icon("warn") == ":warning:"
    assert SlackMessageBuilder.get_slack_status_icon("error") == ":x:"
    assert (
        SlackMessageBuilder.get_slack_status_icon("anything else")
        == ":small_red_triangle:"
    )


def test_get_slack_message():
    slack_message_builder = SlackMessageBuilder()
    slack_message = slack_message_builder.get_slack_message()
    assert isinstance(slack_message, SlackMessageSchema)
    assert json.dumps(slack_message.dict(), sort_keys=True) == json.dumps(
        {"blocks": [], "attachments": [{"blocks": []}], "text": None}, sort_keys=True
    )


def test_add_blocks_as_attachments():
    slack_message_builder = SlackMessageBuilder()
    first_block = slack_message_builder.create_divider_block()
    second_block = slack_message_builder.create_empty_section_block()
    slack_message_builder._add_blocks_as_attachments([first_block])
    slack_message_builder._add_blocks_as_attachments([second_block])
    slack_message = slack_message_builder.get_slack_message()
    assert json.dumps(slack_message.dict(), sort_keys=True) == json.dumps(
        {
            "blocks": [],
            "attachments": [
                {
                    "blocks": [
                        {"type": "divider"},
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
            "text": None,
        },
        sort_keys=True,
    )


def test_add_always_displayed_blocks():
    slack_message_builder = SlackMessageBuilder()
    first_block = slack_message_builder.create_divider_block()
    second_block = slack_message_builder.create_empty_section_block()
    slack_message_builder._add_always_displayed_blocks([first_block])
    slack_message_builder._add_always_displayed_blocks([second_block])
    slack_message = slack_message_builder.get_slack_message()
    assert json.dumps(slack_message.dict(), sort_keys=True) == json.dumps(
        {
            "blocks": [
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "\t",
                    },
                },
            ],
            "attachments": [{"blocks": []}],
            "text": None,
        },
        sort_keys=True,
    )


def test_get_limited_markdown_msg():
    slack_message_builder = SlackMessageBuilder()
    short_message = "short message"
    long_message = short_message * 3000

    markdown_short_message = slack_message_builder.get_limited_markdown_msg(
        short_message
    )
    markdown_long_message = slack_message_builder.get_limited_markdown_msg(long_message)
    assert markdown_short_message == short_message
    assert len(markdown_long_message) == SectionBlock.text_max_length
    assert markdown_long_message.endswith("...age")


@pytest.mark.parametrize(
    "text, url",
    [
        ("first", "https://example1.com"),
        ("second", "https://example2.com"),
        ("third", "https://example3.com"),
    ],
)
def test_create_button_action_block(text, url):
    button_action_block = SlackMessageBuilder.create_button_action_block(
        text=text, url=url
    )
    assert json.dumps(button_action_block, sort_keys=True) == json.dumps(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": text, "emoji": True},
                    "value": text,
                    "url": url,
                }
            ],
        },
        sort_keys=True,
    )


def test_prettify_and_dedup_list():
    message_builder = SlackMessageBuilder()
    list_prettified = message_builder.prettify_and_dedup_list(
        ["name1", "name2", "name2"]
    )
    assert list_prettified == "name1, name2"

    assert (
        message_builder.prettify_and_dedup_list("name1, name2, name2") == "name1, name2"
    )

    string_of_list_prettified = message_builder.prettify_and_dedup_list(
        '["name1", "name2", "name2"]'
    )
    assert string_of_list_prettified == "name1, name2"


def test_slack_message_attachments_limit():
    very_short_attachments = ["attachment"] * (
        SlackMessageBuilder._MAX_AMOUNT_OF_ATTACHMENTS - 1
    )
    short_attachments = ["attachment"] * SlackMessageBuilder._MAX_AMOUNT_OF_ATTACHMENTS
    long_attachments = ["attachment"] * (
        SlackMessageBuilder._MAX_AMOUNT_OF_ATTACHMENTS + 1
    )

    assert (
        SlackMessageSchema(attachments=very_short_attachments).attachments
        == very_short_attachments
    )
    assert (
        SlackMessageSchema(attachments=short_attachments).attachments
        == short_attachments
    )
    assert SlackMessageSchema(attachments=long_attachments).attachments is None
    assert SlackMessageSchema(attachments=[]).attachments == []
