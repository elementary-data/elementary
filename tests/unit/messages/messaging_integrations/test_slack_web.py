import json
from http.client import IncompleteRead
from unittest.mock import MagicMock, patch

import pytest
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError, SlackRequestError

from elementary.messages.blocks import Icon, LineBlock, LinesBlock, MentionBlock
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    ReactionAction,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationDestinationNotFoundError,
    MessagingIntegrationError,
)
from elementary.messages.messaging_integrations.slack_web import (
    ICON_TO_SLACK_EMOJI_NAME,
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


def test_send_raises_channel_not_found_error():
    integration = _build_integration()
    integration.client.chat_postMessage.side_effect = SlackApiError(
        "error", MagicMock(data={"error": "channel_not_found"})
    )
    with pytest.raises(MessagingIntegrationDestinationNotFoundError) as exc_info:
        integration.send_message("alerts", HANDLE_MENTION_BODY)
    assert "alerts" in str(exc_info.value)
    assert isinstance(exc_info.value, MessagingIntegrationError)


def test_send_raises_generic_error_for_other_slack_errors():
    integration = _build_integration()
    integration.client.chat_postMessage.side_effect = SlackApiError(
        "error", MagicMock(data={"error": "is_archived"})
    )
    with pytest.raises(MessagingIntegrationError) as exc_info:
        integration.send_message("alerts", HANDLE_MENTION_BODY)
    assert not isinstance(exc_info.value, MessagingIntegrationDestinationNotFoundError)


NOT_IN_CHANNEL_ERROR = SlackApiError(
    "error", MagicMock(data={"error": "not_in_channel"})
)


def _build_not_in_channel_integration() -> SlackWebMessagingIntegration:
    integration = _build_integration()
    integration.client.conversations_list.return_value = {
        "channels": [{"id": "C_ALERTS", "name": "alerts"}],
        "response_metadata": {"next_cursor": ""},
    }
    return integration


@pytest.mark.parametrize("reply", [False, True])
def test_send_joins_channel_and_retries_on_not_in_channel(reply):
    integration = _build_not_in_channel_integration()
    integration.reply_broadcast = True
    integration.client.chat_postMessage.side_effect = [
        NOT_IN_CHANNEL_ERROR,
        {"ts": "123.456", "channel": "C_ALERTS"},
    ]
    if reply:
        context = SlackWebMessageContext(id="111.222", channel="C_ALERTS")
        result = integration.reply_to_message("alerts", context, HANDLE_MENTION_BODY)
    else:
        result = integration.send_message("alerts", HANDLE_MENTION_BODY)
    assert result.message_context.id == "123.456"
    integration.client.conversations_join.assert_called_once_with(channel="C_ALERTS")
    first_call, retry_call = integration.client.chat_postMessage.call_args_list
    assert retry_call.kwargs == first_call.kwargs


def test_send_raises_when_still_not_in_channel_after_join():
    integration = _build_not_in_channel_integration()
    integration.client.chat_postMessage.side_effect = NOT_IN_CHANNEL_ERROR
    with pytest.raises(MessagingIntegrationError):
        integration.send_message("alerts", HANDLE_MENTION_BODY)
    integration.client.conversations_join.assert_called_once()
    assert integration.client.chat_postMessage.call_count == 2


REACTION_CONTEXT = SlackWebMessageContext(id="123.456", channel="C_ALERTS")


def test_react_adds_emoji_to_message():
    integration = _build_integration()
    integration.react_to_message("alerts", REACTION_CONTEXT, Icon.CHECK)
    integration.client.reactions_add.assert_called_once_with(
        channel="C_ALERTS", timestamp="123.456", name="white_check_mark"
    )
    integration.client.reactions_remove.assert_not_called()


def test_react_removes_emoji_from_message():
    integration = _build_integration()
    integration.react_to_message(
        "alerts", REACTION_CONTEXT, Icon.CHECK, ReactionAction.REMOVE
    )
    integration.client.reactions_remove.assert_called_once_with(
        channel="C_ALERTS", timestamp="123.456", name="white_check_mark"
    )
    integration.client.reactions_add.assert_not_called()


def test_react_maps_every_icon_to_an_emoji_name():
    assert set(ICON_TO_SLACK_EMOJI_NAME) == set(Icon)


def test_react_skips_icon_without_emoji_name(monkeypatch):
    monkeypatch.delitem(ICON_TO_SLACK_EMOJI_NAME, Icon.CHECK)
    integration = _build_integration()
    integration.react_to_message("alerts", REACTION_CONTEXT, Icon.CHECK)
    integration.client.reactions_add.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        "missing_scope",
        "already_reacted",
        "no_reaction",
        "message_not_found",
        "is_archived",
        "ratelimited",
    ],
)
def test_react_does_not_raise_on_slack_errors(error):
    integration = _build_integration()
    integration.client.reactions_add.side_effect = SlackApiError(
        "error", MagicMock(data={"error": error})
    )
    integration.react_to_message("alerts", REACTION_CONTEXT, Icon.CHECK)
    integration.client.reactions_add.assert_called_once()


@pytest.mark.parametrize(
    "exception",
    [
        SlackRequestError("request failed"),
        TimeoutError("timed out"),
        IncompleteRead(b"partial"),
        json.JSONDecodeError("Expecting value", '{"ok":', 6),
        SlackApiError("error", MagicMock(data={"ok": False, "error": []})),
        SlackApiError("error", {"status": 502, "headers": {}, "body": "<html>"}),
    ],
)
def test_react_tracks_and_warns_on_request_errors(exception):
    tracking = MagicMock()
    integration = SlackWebMessagingIntegration(MagicMock(), tracking)
    integration.client.reactions_add.side_effect = exception
    with patch(
        "elementary.messages.messaging_integrations.slack_web.logger"
    ) as mock_logger:
        integration.react_to_message("alerts", REACTION_CONTEXT, Icon.CHECK)
    integration.client.reactions_add.assert_called_once()
    tracking.record_internal_exception.assert_called_once_with(exception)
    mock_logger.warning.assert_called_once()


def test_react_records_unexpected_slack_errors_only():
    tracking = MagicMock()
    integration = SlackWebMessagingIntegration(MagicMock(), tracking)
    integration.client.reactions_add.side_effect = [
        SlackApiError("error", MagicMock(data={"error": "already_reacted"})),
        SlackApiError("error", MagicMock(data={"error": "ratelimited"})),
    ]
    integration.react_to_message("alerts", REACTION_CONTEXT, Icon.CHECK)
    tracking.record_internal_exception.assert_not_called()
    integration.react_to_message("alerts", REACTION_CONTEXT, Icon.CHECK)
    tracking.record_internal_exception.assert_called_once()


def _build_bare_owner_integration() -> SlackWebMessagingIntegration:
    client = MagicMock()
    client.users_list.return_value = {
        "members": [
            {
                "id": "U_ADA",
                "name": "adauser",
                "profile": {"email": "ada@example.com", "display_name": "countess"},
            }
        ]
    }

    def lookup_by_email(email):
        if email == "ada@example.com":
            return {"user": {"id": "U_ADA"}}
        raise SlackApiError("error", MagicMock(data={"error": "users_not_found"}))

    client.users_lookupByEmail.side_effect = lookup_by_email
    client.usergroups_list.return_value = {
        "usergroups": [{"id": "S_DATA_TEAM", "handle": "data-team"}]
    }
    client.chat_postMessage.return_value = {"ts": "123.456", "channel": "C1"}
    return SlackWebMessagingIntegration(client)


def test_resolve_user_id_full_email_uses_email_lookup():
    integration = _build_bare_owner_integration()
    assert integration.resolve_user_id("ada@example.com") == "U_ADA"
    integration.client.users_lookupByEmail.assert_called_once_with(
        email="ada@example.com"
    )
    integration.client.users_list.assert_not_called()


@pytest.mark.parametrize("owner", ["@ada", "ada", "Ada", "countess"])
def test_resolve_user_id_bare_owner_as_handle(owner):
    integration = _build_bare_owner_integration()
    assert integration.resolve_user_id(owner) == "U_ADA"
    integration.client.users_lookupByEmail.assert_not_called()


def test_resolve_user_id_bare_owner_as_usergroup_handle():
    integration = _build_bare_owner_integration()
    assert integration.resolve_user_id("data-team") == "S_DATA_TEAM"
    integration.client.users_lookupByEmail.assert_not_called()


def test_resolve_user_id_bare_owner_matching_nothing():
    integration = _build_bare_owner_integration()
    assert integration.resolve_user_id("not-a-person") is None
    integration.client.users_lookupByEmail.assert_not_called()


def test_send_resolves_bare_owner_mentions():
    integration = _build_bare_owner_integration()
    body = MessageBody(
        blocks=[
            LinesBlock(
                lines=[
                    LineBlock(
                        inlines=[
                            MentionBlock(user="ada"),
                            MentionBlock(user="not-a-person"),
                        ],
                        sep=", ",
                    )
                ]
            )
        ]
    )
    integration.send_message("C1", body)
    sent = integration.client.chat_postMessage.call_args.kwargs
    rendered = sent["blocks"] + sent["attachments"]
    assert "<@U_ADA>, not-a-person" in rendered


def test_react_tracks_and_warns_on_non_object_json_response():
    tracking = MagicMock()
    integration = SlackWebMessagingIntegration(WebClient(token="xoxb-test"), tracking)
    with patch.object(
        WebClient,
        "_perform_urllib_http_request",
        return_value={"status": 200, "headers": {}, "body": "[1]"},
    ), patch(
        "elementary.messages.messaging_integrations.slack_web.logger"
    ) as mock_logger:
        integration.react_to_message("alerts", REACTION_CONTEXT, Icon.CHECK)
    tracking.record_internal_exception.assert_called_once()
    assert isinstance(
        tracking.record_internal_exception.call_args.args[0], AttributeError
    )
    mock_logger.warning.assert_called_once()
