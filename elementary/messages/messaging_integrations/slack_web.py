import json
import re
import ssl
import time
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Dict, Iterator, List, Optional, Tuple

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
from elementary.utils.pydantic_shim import BaseModel

logger = get_logger(__name__)

ONE_MINUTE = 60
ONE_SECOND = 1

_CHANNEL_ID_PATTERN = re.compile(r"^[CGD][A-Z0-9]{8,}$")


def _is_channel_id(value: str) -> bool:
    return bool(_CHANNEL_ID_PATTERN.match(value))


def _normalize_channel_input(raw: str) -> str:
    normalized = raw.strip()
    if normalized.startswith("#"):
        normalized = normalized[1:].strip()
    return normalized


@dataclass
class ResolvedChannel:
    name: str
    id: str


@dataclass
class ChannelsResponse:
    channels: list[ResolvedChannel]
    retry_after: int | None
    cursor: str | None


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
        self._channel_cache: Dict[Tuple[str, bool], ResolvedChannel] = {}
        self.reply_broadcast = reply_broadcast

    @classmethod
    def from_token(
        cls,
        token: str,
        tracking: Optional[Tracking] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        **kwargs: Any,
    ) -> "SlackWebMessagingIntegration":
        client = WebClient(token=token, ssl=ssl_context)
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
            channel_id = self.resolve_channel(channel_name, only_public=True).id
            self._join_channel(channel_id=channel_id)
            logger.info(f"Joined channel {channel_name}")
        elif err_type == "channel_not_found":
            raise MessagingIntegrationError(
                f"Channel {channel_name} was not found by the Elementary app. Please add the app to the channel."
            )
        raise MessagingIntegrationError(
            f"Failed to send a message to channel - {channel_name}"
        )

    def _list_conversations(
        self, cursor: Optional[str] = None
    ) -> Tuple[List[dict], Optional[str]]:
        response = self.client.conversations_list(
            cursor=cursor,
            types="public_channel,private_channel",
            exclude_archived=True,
            limit=1000,
        )
        channels = response.get("channels", [])
        cursor = response.get("response_metadata", {}).get("next_cursor")
        return channels, cursor

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
        channels, cursor = self._list_conversations(cursor)
        call_duration = time.time() - call_start

        yield from channels
        if cursor:
            timeout_left = timeout - call_duration
            yield from self._iter_channels(cursor, only_public, timeout_left)

    @sleep_and_retry
    @limits(calls=50, period=ONE_MINUTE)
    def resolve_channel(
        self, channel: str, only_public: bool = False
    ) -> ResolvedChannel:
        normalized = _normalize_channel_input(channel)
        cache_key = (normalized, only_public)
        if cache_key in self._channel_cache:
            return self._channel_cache[cache_key]

        if _is_channel_id(normalized):
            try:
                response = self.client.conversations_info(channel=normalized)
            except SlackApiError as e:
                if self.tracking:
                    self.tracking.record_internal_exception(e)
                raise MessagingIntegrationError(
                    f"Channel {normalized} not found"
                ) from e
            ch = response["channel"]
            resolved = ResolvedChannel(name=ch["name"], id=ch["id"])
        else:
            for ch in self._iter_channels(only_public=only_public):
                if ch["name"] == normalized:
                    resolved = ResolvedChannel(name=ch["name"], id=ch["id"])
                    break
            else:
                raise MessagingIntegrationError(f"Channel {normalized} not found")

        self._channel_cache[cache_key] = resolved
        return resolved

    def get_channels(
        self,
        cursor: str | None = None,
        timeout_seconds: int = 15,
    ) -> ChannelsResponse:
        channels_response = ChannelsResponse(channels=[], retry_after=None, cursor=None)

        start_time = time.time()
        time_elapsed: float = 0
        while time_elapsed < timeout_seconds:
            try:
                channels, cursor = self._list_conversations(cursor)
                time_elapsed = time.time() - start_time
                logger.debug(
                    f"Got a batch of {len(channels)} channels! time elapsed: {time_elapsed} seconds"
                )

                channels_response.channels.extend(
                    [
                        ResolvedChannel(name=chan["name"], id=chan["id"])
                        for chan in channels
                    ]
                )

                if not cursor:
                    break

            except SlackApiError as err:
                if err.response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
                    channels_response.retry_after = int(
                        err.response.headers["Retry-After"]
                    )
                    break
                raise

        channels_response.cursor = cursor
        return channels_response

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
