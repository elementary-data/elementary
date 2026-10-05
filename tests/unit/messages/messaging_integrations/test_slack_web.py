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
            "name": "jessicajones",
            "profile": {
                "email": "jessica.jones@marvel.com",
                "display_name": "jjones",
            },
        },
        {
            "id": "U_BOT",
            "name": "botuser",
            "is_bot": True,
            "profile": {"display_name": "bot"},
        },
    ],
    "response_metadata": {"next_cursor": "page2"},
}
USERS_PAGE_2 = {
    "members": [
        {
            "id": "U_LUKE",
            "name": "lukecage",
            "profile": {"email": "lcage@marvel.com", "display_name": "luke"},
        },
        {
            "id": "U_DELETED",
            "name": "deleteduser",
            "deleted": True,
            "profile": {"email": "deleted@marvel.com", "display_name": "deleted"},
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
    client.usergroups_list.return_value = {"usergroups": []}
    return SlackWebMessagingIntegration(client)


def test_resolve_user_id_by_email_prefix_handle():
    integration = _build_integration()
    assert integration.resolve_user_id("@jessica.jones") == "U_JESSICA"
    assert integration.resolve_user_id("@Jessica.Jones") == "U_JESSICA"


def test_resolve_user_id_by_username_handle():
    integration = _build_integration()
    assert integration.resolve_user_id("@jjones") == "U_JESSICA"
    assert integration.resolve_user_id("@JJones") == "U_JESSICA"
    assert integration.resolve_user_id("@luke") == "U_LUKE"
    # Slack `name` is ignored; only profile.display_name resolves.
    assert integration.resolve_user_id("@jessicajones") is None
    assert integration.resolve_user_id("@lukecage") is None


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
                "name": "guestjohn",
                "is_restricted": True,
                "profile": {"email": "john@partner.com", "display_name": "john"},
            },
            {
                "id": "U_JOHN",
                "name": "johnuser",
                "profile": {"email": "john@company.com", "display_name": "jdoe"},
            },
            {
                "id": "U_JOHN_2",
                "name": "jduser",
                "profile": {"email": "john@other.com", "display_name": "jd"},
            },
            {
                "id": "U_JANE",
                "name": "janeuser",
                "profile": {"email": "jd@company.com", "display_name": "jane.doe"},
            },
        ],
    }
    # Full members win over guests, then the first listed user wins.
    assert integration.resolve_user_id("@john") == "U_JOHN"
    # Email prefix matches win over username matches.
    assert integration.resolve_user_id("@jd") == "U_JANE"


def test_resolve_user_id_by_usergroup_handle():
    integration = _build_integration()
    integration.client.usergroups_list.return_value = {
        "usergroups": [
            {"id": "S_NO_HANDLE"},
            {"id": "S_DATA", "handle": "data"},
            {"id": "S_DATA_LATER", "handle": "data"},
        ]
    }
    assert integration.resolve_user_id("@data") == "S_DATA"
    assert integration.resolve_user_id("@Data") == "S_DATA"
    integration.resolve_user_id("@data")
    assert integration.client.usergroups_list.call_count == 1


def test_send_resolves_usergroup_mentions():
    integration = _build_integration()
    integration.client.usergroups_list.return_value = {
        "usergroups": [{"id": "S_DATA", "handle": "data"}]
    }
    integration.client.chat_postMessage.return_value = {
        "ts": "123.456",
        "channel": "C1",
    }
    body = MessageBody(
        blocks=[LinesBlock(lines=[LineBlock(inlines=[MentionBlock(user="@data")])])]
    )
    integration.send_message("C1", body)
    sent = integration.client.chat_postMessage.call_args.kwargs
    assert "<!subteam^S_DATA>" in sent["blocks"] + sent["attachments"]


def test_user_handle_wins_over_usergroup():
    integration = _build_integration()
    integration.client.usergroups_list.return_value = {
        "usergroups": [{"id": "S_JJONES", "handle": "jjones"}]
    }
    assert integration.resolve_user_id("@jjones") == "U_JESSICA"
    integration.client.usergroups_list.assert_not_called()


def test_usergroups_list_failure_does_not_break_user_resolution():
    integration = _build_integration()
    integration.client.usergroups_list.side_effect = SlackApiError(
        "error", MagicMock(data={"error": "missing_scope"})
    )
    assert integration.resolve_user_id("@jessica.jones") == "U_JESSICA"
    integration.client.usergroups_list.assert_not_called()
    assert integration.resolve_user_id("@data") is None
    assert integration.resolve_user_id("@data") is None
    assert integration.client.usergroups_list.call_count == 1


def test_resolve_user_id_when_users_list_cursor_does_not_advance():
    integration = _build_integration()
    integration.client.users_list.side_effect = lambda cursor=None, limit=None: {
        "members": USERS_PAGE_1["members"],
        "response_metadata": {"next_cursor": "stuck"},
    }
    assert integration.resolve_user_id("@jessica.jones") == "U_JESSICA"
    assert integration.client.users_list.call_count == 2
