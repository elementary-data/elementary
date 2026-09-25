from unittest.mock import MagicMock

import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.web import SlackResponse

from elementary.messages.blocks import HeaderBlock
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)
from elementary.messages.messaging_integrations.slack_web import (
    SlackWebMessagingIntegration,
)

CHANNEL_NAME = "data-alerts"
CHANNEL_ID = "C123"


def _slack_error(error: str) -> SlackApiError:
    response = SlackResponse(
        client=None,
        http_verb="POST",
        api_url="https://slack.com/api/chat.postMessage",
        req_args={},
        data={"ok": False, "error": error},
        headers={},
        status_code=200,
    )
    return SlackApiError(message=error, response=response)


def _posted_message() -> dict:
    return {"ts": "1700000000.000100", "channel": CHANNEL_ID}


def _create_integration(client: MagicMock) -> SlackWebMessagingIntegration:
    client.conversations_list.return_value = {
        "channels": [{"name": CHANNEL_NAME, "id": CHANNEL_ID}],
        "response_metadata": {"next_cursor": ""},
    }
    return SlackWebMessagingIntegration(client=client, reply_broadcast=True)


def _body() -> MessageBody:
    return MessageBody(blocks=[HeaderBlock(text="Test alert")])


def test_send_message_joins_channel_and_retries_when_not_in_channel():
    client = MagicMock()
    client.chat_postMessage.side_effect = [
        _slack_error("not_in_channel"),
        _posted_message(),
    ]
    integration = _create_integration(client)

    result = integration.send_message(destination=CHANNEL_NAME, body=_body())

    client.conversations_join.assert_called_once_with(channel=CHANNEL_ID)
    assert client.chat_postMessage.call_count == 2
    assert result.message_context.id == "1700000000.000100"
    assert result.message_context.channel == CHANNEL_ID


def test_reply_retry_after_join_keeps_thread_and_reply_broadcast():
    client = MagicMock()
    client.chat_postMessage.side_effect = [
        _slack_error("not_in_channel"),
        _posted_message(),
    ]
    integration = _create_integration(client)
    context = integration.parse_message_context(
        {"id": "1690000000.000100", "channel": CHANNEL_ID}
    )

    integration.reply_to_message(
        destination=CHANNEL_NAME, message_context=context, body=_body()
    )

    retry_kwargs = client.chat_postMessage.call_args_list[1].kwargs
    assert retry_kwargs["thread_ts"] == "1690000000.000100"
    assert retry_kwargs["reply_broadcast"] is True


def test_send_message_raises_if_still_not_in_channel_after_join():
    client = MagicMock()
    client.chat_postMessage.side_effect = _slack_error("not_in_channel")
    integration = _create_integration(client)

    with pytest.raises(MessagingIntegrationError):
        integration.send_message(destination=CHANNEL_NAME, body=_body())

    client.conversations_join.assert_called_once_with(channel=CHANNEL_ID)
    assert client.chat_postMessage.call_count == 2


def test_send_message_raises_on_channel_not_found():
    client = MagicMock()
    client.chat_postMessage.side_effect = _slack_error("channel_not_found")
    integration = _create_integration(client)

    with pytest.raises(MessagingIntegrationError):
        integration.send_message(destination=CHANNEL_NAME, body=_body())

    client.conversations_join.assert_not_called()
    assert client.chat_postMessage.call_count == 1
