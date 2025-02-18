from datetime import datetime
from http import HTTPStatus
from typing import Optional

from pydantic import BaseModel
from ratelimit import limits, sleep_and_retry
from slack_sdk import WebhookClient
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

from elementary.messages.formats.block_kit import (
    FormattedBlockKitMessage,
    format_block_kit,
)
from elementary.messages.message_body import MessageBody
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    MessageSendResult,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)
from elementary.tracking.tracking_interface import Tracking

ONE_SECOND = 1


class SlackWebhookDestination(BaseModel):
    webhook: str


class SlackWebhookMessagingIntegration(
    BaseMessagingIntegration[SlackWebhookDestination, SlackWebhookDestination]
):
    def __init__(self, tracking: Optional[Tracking] = None) -> None:
        self.tracking = tracking

    def _get_client(self, destination: SlackWebhookDestination) -> WebhookClient:
        client = WebhookClient(destination.webhook)
        client.retry_handlers.append(RateLimitErrorRetryHandler(max_retry_count=5))
        return client

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def _send_message(
        self, client: WebhookClient, formatted_message: FormattedBlockKitMessage
    ) -> None:
        response = client.send(
            blocks=formatted_message.blocks,
            attachments=formatted_message.attachments,
        )
        if response.status_code != HTTPStatus.OK:
            raise MessagingIntegrationError(
                f"Could not post message to slack via webhook - {client.url}. Status code: {response.status_code}, Error: {response.body}"
            )

    def send_message(
        self, destination: SlackWebhookDestination, body: MessageBody
    ) -> MessageSendResult[SlackWebhookDestination]:
        formatted_message = format_block_kit(body)
        client = self._get_client(destination)
        self._send_message(client, formatted_message)
        return MessageSendResult(
            message_context=destination,
            timestamp=datetime.utcnow(),
        )

    def supports_reply(self) -> bool:
        return False
