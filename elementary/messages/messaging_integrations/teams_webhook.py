from datetime import datetime
from typing import Optional

import requests
from typing_extensions import TypeAlias

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    MessageSendResult,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


Channel: TypeAlias = Optional[str]


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
    if response.status_code == 202:
        logger.debug("Got 202 response from Teams webhook, assuming success")
    return response


class TeamsWebhookMessagingIntegration(BaseMessagingIntegration[Channel, Channel]):
    def __init__(self, url: str) -> None:
        self.url = url

    def send_message(
        self,
        destination: Channel,
        body: MessageBody,
    ) -> MessageSendResult[Channel]:
        card = format_adaptive_card(body)
        try:
            send_adaptive_card(self.url, card)
            return MessageSendResult(
                message_context=destination,
                timestamp=datetime.utcnow(),
            )
        except requests.RequestException as e:
            raise MessagingIntegrationError(
                "Failed to send message to Teams webhook"
            ) from e

    def supports_reply(self) -> bool:
        return False
