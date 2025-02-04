from datetime import datetime
from typing import Optional

import requests
from pydantic import BaseModel

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    MessageSendResult,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessageIntegrationReplyNotSupportedError,
    MessagingIntegrationError,
)
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class ChannelWebhook(BaseModel):
    webhook: str
    channel: Optional[str] = None


def send_adaptive_card(webhook_url: str, card: dict) -> requests.Response:
    """Sends an Adaptive Card to the specified webhook URL."""
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


class TeamsWebhookMessagingIntegration(
    BaseMessagingIntegration[ChannelWebhook, ChannelWebhook]
):
    def send_message(
        self,
        destination: ChannelWebhook,
        message_body: MessageBody,
    ) -> MessageSendResult[ChannelWebhook]:
        card = format_adaptive_card(message_body)
        try:
            send_adaptive_card(destination.webhook, card)
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

    def reply_to_message(
        self,
        destination: ChannelWebhook,
        message_context: ChannelWebhook,
        message_body: MessageBody,
    ) -> MessageSendResult[ChannelWebhook]:
        raise MessageIntegrationReplyNotSupportedError(
            "Teams webhook message integration does not support replying to messages"
        )
