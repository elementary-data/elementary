from datetime import datetime
from http import HTTPStatus
from typing import Any, Optional

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
from elementary.messages.messaging_integrations.empty_message_context import (
    EmptyMessageContext,
)
from elementary.messages.messaging_integrations.exceptions import (
    MessagingIntegrationError,
)
from elementary.tracking.tracking_interface import Tracking

ONE_SECOND = 1


class SlackWebhookMessagingIntegration(
    BaseMessagingIntegration[None, EmptyMessageContext]
):
    def __init__(
        self, client: WebhookClient, tracking: Optional[Tracking] = None
    ) -> None:
        self.client = client
        self.tracking = tracking

    @classmethod
    def from_url(
        cls, url: str, tracking: Optional[Tracking] = None
    ) -> "SlackWebhookMessagingIntegration":
        client = WebhookClient(url)
        client.retry_handlers.append(RateLimitErrorRetryHandler(max_retry_count=5))
        return cls(client, tracking)

    def parse_message_context(self, context: dict[str, Any]) -> EmptyMessageContext:
        return EmptyMessageContext(**context)

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def _send_message(self, formatted_message: FormattedBlockKitMessage) -> None:
        response = self.client.send(
            blocks=formatted_message.blocks,
            attachments=formatted_message.attachments,
        )
        if response.status_code != HTTPStatus.OK:
            raise MessagingIntegrationError(
                f"Could not post message to slack via webhook - {self.client.url}. Status code: {response.status_code}, Error: {response.body}"
            )

    def send_message(
        self, destination: None, body: MessageBody
    ) -> MessageSendResult[EmptyMessageContext]:
        formatted_message = format_block_kit(body)
        self._send_message(formatted_message)
        return MessageSendResult(
            message_context=EmptyMessageContext(),
            timestamp=datetime.utcnow(),
            message_format="block_kit",
        )

    def supports_reply(self) -> bool:
        return False
