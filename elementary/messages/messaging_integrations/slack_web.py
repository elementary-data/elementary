import json
import time
from typing import Any, Dict, Iterator, Optional

from pydantic import BaseModel
from ratelimit import limits, sleep_and_retry
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler
from typing_extensions import TypeAlias

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
from elementary.utils.log import get_logger

logger = get_logger(__name__)

ONE_MINUTE = 60
ONE_SECOND = 1


Channel: TypeAlias = str


class SlackWebMessageContext(BaseModel):
    id: str
    channel: Channel


class SlackWebMessagingIntegration(
    BaseMessagingIntegration[Channel, SlackWebMessageContext]
):
    def __init__(
        self,
        client: WebClient,
        tracking: Optional[Tracking] = None,
        reply_broadcast: bool = False,
    ) -> None:
        self.client = client
        self.tracking = tracking
        self._email_to_user_id_cache: Dict[str, str] = {}
        self.reply_broadcast = reply_broadcast

    @classmethod
    def from_token(
        cls, token: str, tracking: Optional[Tracking] = None, **kwargs: Any
    ) -> "SlackWebMessagingIntegration":
        client = WebClient(token=token)
        client.retry_handlers.append(RateLimitErrorRetryHandler(max_retry_count=5))
        return cls(client, tracking, **kwargs)

    def parse_message_context(self, context: dict[str, Any]) -> SlackWebMessageContext:
        return SlackWebMessageContext(**context)

    def supports_reply(self) -> bool:
        return True

    def supports_actions(self) -> bool:
        return True

    def send_message(
        self, destination: Channel, body: MessageBody
    ) -> MessageSendResult[SlackWebMessageContext]:
        formatted_message = format_block_kit(body, self.get_user_id_from_email)
        return self._send_message(destination, formatted_message)

    def reply_to_message(
        self,
        destination: Channel,
        message_context: SlackWebMessageContext,
        body: MessageBody,
    ) -> MessageSendResult[SlackWebMessageContext]:
        formatted_message = format_block_kit(body, self.get_user_id_from_email)
        return self._send_message(
            destination,
            formatted_message,
            thread_ts=message_context.id,
            reply_broadcast=self.reply_broadcast,
        )

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def _send_message(
        self,
        destination: Channel,
        formatted_message: FormattedBlockKitMessage,
        thread_ts: Optional[str] = None,
        reply_broadcast: bool = False,
    ) -> MessageSendResult[SlackWebMessageContext]:
        try:
            response = self.client.chat_postMessage(
                channel=destination,
                blocks=json.dumps(formatted_message.blocks),
                attachments=json.dumps(formatted_message.attachments),
                thread_ts=thread_ts,
                unfurl_links=False,
                reply_broadcast=reply_broadcast,
            )
        except SlackApiError as e:
            self._handle_send_err(e, destination)
            return self._send_message(destination, formatted_message, thread_ts)

        return MessageSendResult(
            message_context=SlackWebMessageContext(
                id=response["ts"], channel=response["channel"]
            ),
            timestamp=response["ts"],
            message_format="block_kit",
        )

    def _handle_send_err(self, err: SlackApiError, channel_name: str):
        if self.tracking:
            self.tracking.record_internal_exception(err)
        err_type = err.response.data["error"]
        if err_type == "not_in_channel":
            logger.info(
                f'Elementary app is not in the channel "{channel_name}". Attempting to join.'
            )
            channel_id = self._get_channel_id(channel_name, only_public=True)
            self._join_channel(channel_id=channel_id)
            logger.info(f"Joined channel {channel_name}")
        elif err_type == "channel_not_found":
            raise MessagingIntegrationError(
                f"Channel {channel_name} was not found by the Elementary app. Please add the app to the channel."
            )
        raise MessagingIntegrationError(
            f"Failed to send a message to channel - {channel_name}"
        )

    @sleep_and_retry
    @limits(calls=20, period=ONE_MINUTE)
    def _iter_channels(
        self,
        cursor: Optional[str] = None,
        only_public: bool = False,
        timeout: float = 300.0,
    ) -> Iterator[dict]:
        if timeout <= 0:
            raise MessagingIntegrationError("Channel iteration timed out")

        call_start = time.time()
        response = self.client.conversations_list(
            cursor=cursor,
            types="public_channel" if only_public else "public_channel,private_channel",
            exclude_archived=True,
            limit=1000,
        )
        call_duration = time.time() - call_start

        channels = response["channels"]
        yield from channels
        response_metadata = response.get("response_metadata") or {}
        next_cursor = response_metadata.get("next_cursor")
        if next_cursor:
            if not isinstance(next_cursor, str):
                raise ValueError("Next cursor is not a string")
            timeout_left = timeout - call_duration
            yield from self._iter_channels(next_cursor, only_public, timeout_left)

    def _get_channel_id(self, channel_name: str, only_public: bool = False) -> str:
        for channel in self._iter_channels(only_public=only_public):
            if channel["name"] == channel_name:
                return channel["id"]
        raise MessagingIntegrationError(f"Channel {channel_name} not found")

    def _join_channel(self, channel_id: str) -> None:
        try:
            self.client.conversations_join(channel=channel_id)
        except SlackApiError as e:
            if self.tracking:
                self.tracking.record_internal_exception(e)
            raise MessagingIntegrationError(f"Failed to join channel {channel_id}")

    @sleep_and_retry
    @limits(calls=50, period=ONE_MINUTE)
    def get_user_id_from_email(self, email: str) -> Optional[str]:
        if email in self._email_to_user_id_cache:
            return self._email_to_user_id_cache[email]
        try:
            user_id = self.client.users_lookupByEmail(email=email)["user"]["id"]
            self._email_to_user_id_cache[email] = user_id
            return user_id
        except SlackApiError as err:
            if err.response.data["error"] != "users_not_found":
                logger.error(f"Unable to get Slack user ID from email: {err}.")
            return None
