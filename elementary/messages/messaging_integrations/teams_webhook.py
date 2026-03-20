import json
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any, Dict, List, Optional

import requests
from ratelimit import limits, sleep_and_retry
from typing_extensions import TypeAlias

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    MessageSendResult,
)
from elementary.messages.messaging_integrations.empty_message_context import (
    EmptyMessageContext,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


Channel: TypeAlias = Optional[str]
ONE_SECOND = 1
# Teams webhook payload size limit in bytes. The actual limit is 28KB for
# Adaptive Cards, but we use a slightly lower threshold to leave room for
# the envelope (message wrapper, attachment metadata, etc.).
TEAMS_PAYLOAD_SIZE_LIMIT = 27 * 1024


class TeamsWebhookHttpError(MessagingIntegrationError):
    def __init__(self, response: requests.Response):
        self.status_code = response.status_code
        self.response = response
        super().__init__(
            f"Failed to send message to Teams webhook: {response.status_code}"
        )


def _build_payload(card: dict) -> dict:
    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": card,
            }
        ],
    }


def _truncation_notice_item() -> Dict[str, Any]:
    return {
        "type": "TextBlock",
        "text": "_... Content truncated due to message size limits. "
        "View full details in Elementary Cloud._",
        "wrap": True,
        "isSubtle": True,
    }


def _minimal_card(card: dict) -> dict:
    """Return a minimal card with just a truncation notice when even a single
    body item is too large."""
    return {
        **card,
        "body": [
            {
                "type": "TextBlock",
                "text": "Alert content too large to display in Teams. "
                "View full details in Elementary Cloud.",
                "wrap": True,
                "weight": "bolder",
            }
        ],
    }


def _truncate_card(card: dict) -> dict:
    """Progressively remove body items from the card until the payload fits
    within the Teams size limit.  A truncation notice is appended so the
    recipient knows content was removed."""
    body: List[Dict[str, Any]] = list(card.get("body", []))
    if not body:
        return card

    while len(body) > 1:
        payload = _build_payload({**card, "body": body + [_truncation_notice_item()]})
        if len(json.dumps(payload)) <= TEAMS_PAYLOAD_SIZE_LIMIT:
            break
        body.pop()  # remove the last body item

    truncated = {**card, "body": body + [_truncation_notice_item()]}
    # If even a single body item plus the notice is too large, fall back to
    # a minimal card.
    if len(json.dumps(_build_payload(truncated))) > TEAMS_PAYLOAD_SIZE_LIMIT:
        return _minimal_card(card)
    return truncated


def send_adaptive_card(webhook_url: str, card: dict) -> requests.Response:
    payload = _build_payload(card)

    # Proactively truncate if the payload exceeds the Teams size limit.
    payload_json = json.dumps(payload)
    if len(payload_json) > TEAMS_PAYLOAD_SIZE_LIMIT:
        logger.warning(
            "Teams webhook payload size (%d bytes) exceeds limit (%d bytes), "
            "truncating card body",
            len(payload_json),
            TEAMS_PAYLOAD_SIZE_LIMIT,
        )
        card = _truncate_card(card)
        payload = _build_payload(card)

    response = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    if response.status_code == HTTPStatus.ACCEPTED:
        logger.debug(
            f"Got {HTTPStatus.ACCEPTED} response from Teams webhook, assuming success"
        )
    return response


class TeamsWebhookMessagingIntegration(
    BaseMessagingIntegration[None, EmptyMessageContext]
):
    def __init__(self, url: str) -> None:
        self.url = url

    def parse_message_context(self, context: dict[str, Any]) -> EmptyMessageContext:
        return EmptyMessageContext(**context)

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def send_message(
        self,
        destination: None,
        body: MessageBody,
    ) -> MessageSendResult[EmptyMessageContext]:
        card = format_adaptive_card(body)
        try:
            response = send_adaptive_card(self.url, card)
            # For the old teams webhook version of Teams simply returning status code 200
            # is not indicating that it was successful.
            # In that version they return some text if it was NOT successful, otherwise
            # they return the number 1 in the text. For the new teams webhook version they always
            # return a 202 and nothing else can be used to determine if the message was
            # sent successfully.
            if response.status_code not in (HTTPStatus.OK, HTTPStatus.ACCEPTED) or (
                response.status_code == HTTPStatus.OK and len(response.text) > 1
            ):
                raise MessagingIntegrationError(
                    f"Could not post message to Teams via webhook. Status code: {response.status_code}, Error: {response.text}"
                )
            return MessageSendResult(
                message_context=EmptyMessageContext(),
                timestamp=datetime.now(tz=timezone.utc),
                message_format="adaptive_cards",
            )
        except requests.HTTPError as e:
            raise TeamsWebhookHttpError(e.response) from e
        except requests.RequestException as e:
            raise MessagingIntegrationError(
                f"An error occurred while posting message to Teams webhook: {str(e)}"
            ) from e

    def supports_reply(self) -> bool:
        return False
