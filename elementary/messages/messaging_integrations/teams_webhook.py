from datetime import datetime
from http import HTTPStatus
from typing import Any, Optional

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


def send_adaptive_card(webhook_url: str, card: dict) -> requests.Response:
    payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": card,
            }
        ],
    }

    response = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
    if response.status_code == HTTPStatus.ACCEPTED:
        logger.debug(
            "Got %s response from Teams webhook, assuming success", HTTPStatus.ACCEPTED
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
            # The status code is not reliable for the older version and 202 for the newer version
            # The response returns 1 in the body for the old version for success and otherwise some text
            # The new version is not reliable to do a check and in the response there is nothing that can be used
            # to determine if the message was sent successfully.
            if (
                response.status_code not in (HTTPStatus.OK, HTTPStatus.ACCEPTED)
                or len(response.text) > 1
            ):
                raise MessagingIntegrationError(
                    f"Could not post message to Teams via webhook. Status code: {response.status_code}, Error: {response.text}"
                )
            return MessageSendResult(
                message_context=EmptyMessageContext(),
                timestamp=datetime.utcnow(),
                message_format="adaptive_cards",
            )
        except requests.RequestException as e:
            raise MessagingIntegrationError(
                f"Network error while posting message to Teams webhook: {str(e)}"
            ) from e

    def supports_reply(self) -> bool:
        return False
