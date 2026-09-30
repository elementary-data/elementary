from unittest.mock import MagicMock

import pytest
from slack_sdk.errors import SlackApiError

from elementary.messages.blocks import LineBlock, LinesBlock, MentionBlock
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.slack_web import (
    SlackWebMessageContext,
    SlackWebMessagingIntegration,
)

USERS_PAGE_1 = {
    "members": [
        {
            "id": "U_JESSICA",
            "name": "jjones",
            "profile": {"email": "jessica.jones@marvel.com"},
        },
        {"id": "U_BOT", "name": "bot", "is_bot": True, "profile": {}},
    ],
    "response_metadata": {"next_cursor": "page2"},
}
USERS_PAGE_2 = {
    "members": [
        {"id": "U_LUKE", "name": "luke", "profile": {"email": "lcage@marvel.com"}},
        {
            "id": "U_DELETED",
            "name": "deleted",
            "deleted": True,
            "profile": {"email": "deleted@marvel.com"},
        },
    ],
    "response_metadata": {"next_cursor": ""},
}


def _build_integration() -> SlackWebMessagingIntegration:
    client = MagicMock()
    client.users_list.side_effect = lambda cursor=None, limit=None: (
        USERS_PAGE_2 if cursor == "page2" else USERS_PAGE_1
    )
    client.users_lookupByEmail.side_effect = lambda email: {
        "user": {"id": f"U_EMAIL_{email}"}
    }
    return SlackWebMessagingIntegration(client)


def test_resolve_user_id_by_email_prefix_handle():
    integration = _build_integration()
    assert integration.resolve_user_id("@jessica.jones") == "U_JESSICA"
    assert integration.resolve_user_id("@Jessica.Jones") == "U_JESSICA"


def test_resolve_user_id_by_username_handle():
    integration = _build_integration()
    assert integration.resolve_user_id("@jjones") == "U_JESSICA"
    assert integration.resolve_user_id("@luke") == "U_LUKE"


def test_resolve_user_id_skips_bots_deleted_and_unknown_handles():
    integration = _build_integration()
    assert integration.resolve_user_id("@bot") is None
    assert integration.resolve_user_id("@deleted") is None
    assert integration.resolve_user_id("@unknown") is None


def test_users_are_listed_once():
    integration = _build_integration()
    integration.resolve_user_id("@jessica.jones")
    integration.resolve_user_id("@luke")
    assert integration.client.users_list.call_count == 2


def test_resolve_user_id_by_email():
    integration = _build_integration()
    assert (
        integration.resolve_user_id("jessica.jones@marvel.com")
        == "U_EMAIL_jessica.jones@marvel.com"
    )
    integration.client.users_list.assert_not_called()


def test_resolve_user_id_when_users_list_fails():
    integration = _build_integration()
    integration.client.users_list.side_effect = SlackApiError(
        "error", MagicMock(data={"error": "missing_scope"})
    )
    assert integration.resolve_user_id("@jessica.jones") is None


HANDLE_MENTION_BODY = MessageBody(
    blocks=[
        LinesBlock(lines=[LineBlock(inlines=[MentionBlock(user="@jessica.jones")])])
    ]
)


def _send(integration: SlackWebMessagingIntegration, reply: bool) -> str:
    integration.client.chat_postMessage.return_value = {
        "ts": "123.456",
        "channel": "C1",
    }
    if reply:
        context = SlackWebMessageContext(id="111.222", channel="C1")
        integration.reply_to_message("C1", context, HANDLE_MENTION_BODY)
    else:
        integration.send_message("C1", HANDLE_MENTION_BODY)
    sent = integration.client.chat_postMessage.call_args.kwargs
    return sent["blocks"] + sent["attachments"]


@pytest.mark.parametrize("reply", [False, True])
def test_send_resolves_handle_mentions(reply):
    integration = _build_integration()
    assert "<@U_JESSICA>" in _send(integration, reply)


@pytest.mark.parametrize("reply", [False, True])
def test_send_keeps_plain_handle_when_users_list_raises(reply):
    integration = _build_integration()
    integration.client.users_list.side_effect = ConnectionError("connection reset")
    sent = _send(integration, reply)
    assert "@jessica.jones" in sent
    assert "<@" not in sent


def test_resolve_user_id_collisions():
    integration = _build_integration()
    integration.client.users_list.side_effect = lambda cursor=None, limit=None: {
        "members": [
            {
                "id": "U_GUEST",
                "name": "john",
                "is_restricted": True,
                "profile": {"email": "john@partner.com"},
            },
            {"id": "U_JOHN", "name": "jdoe", "profile": {"email": "john@company.com"}},
            {"id": "U_JOHN_2", "name": "jd", "profile": {"email": "john@other.com"}},
            {
                "id": "U_JANE",
                "name": "jane.doe",
                "profile": {"email": "jd@company.com"},
            },
        ],
    }
    # Full members win over guests, then the first listed user wins.
    assert integration.resolve_user_id("@john") == "U_JOHN"
    # Email prefix matches win over username matches.
    assert integration.resolve_user_id("@jd") == "U_JANE"


def test_resolve_user_id_when_users_list_cursor_does_not_advance():
    integration = _build_integration()
    integration.client.users_list.side_effect = lambda cursor=None, limit=None: {
        "members": USERS_PAGE_1["members"],
        "response_metadata": {"next_cursor": "stuck"},
    }
    assert integration.resolve_user_id("@jessica.jones") == "U_JESSICA"
    assert integration.client.users_list.call_count == 2
