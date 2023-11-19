import json
import uuid
from datetime import datetime, timedelta
from typing import Optional

from ratelimit import limits, sleep_and_retry  # type: ignore[import]
from slack_sdk.errors import SlackApiError

from elementary.clients.slack.client import (
    ONE_MINUTE,
    ONE_SECOND,
    SlackClient,
    SlackWebClient,
)
from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.tracking.tracking_interface import Tracking


class E2ESlackWebClient(SlackWebClient):
    def __init__(
        self,
        token: str,
        tracking: Optional[Tracking] = None,
    ):
        super().__init__(token, tracking)
        self.unique_id = str(uuid.uuid4())

    @sleep_and_retry
    @limits(calls=50, period=ONE_MINUTE)
    def get_channel_messages(self, channel_name: str, after_hours: float = 24):
        channel_id = self._get_channel_id(channel_name=channel_name)
        min_timestamp = (
            (datetime.utcnow() - timedelta(hours=after_hours)).timestamp()
            if after_hours
            else 0
        )
        cursor = None
        messsges = []
        while True:
            response = self.client.conversations_history(
                channel=channel_id, oldest=min_timestamp, cursor=cursor
            )
            messsges.extend(response["messages"])
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if cursor is None:
                break
        return messsges

    @sleep_and_retry
    @limits(calls=50, period=ONE_MINUTE)
    def get_channel_messages_with_replies(
        self, channel_name: str, after_hours: float = 24
    ):
        channel_id = self._get_channel_id(channel_name=channel_name) or ""
        messages_with_replies = []
        messages = self.get_channel_messages(
            channel_name=channel_name, after_hours=after_hours
        )
        for message in messages:
            messages_with_replies.append(
                self._get_channel_message_replies(
                    channel_id=channel_id, message_ts=message.get("ts")
                )
            )
        return messages_with_replies

    def _get_channel_message_replies(self, channel_id: str, message_ts: str):
        response = self.client.conversations_replies(channel=channel_id, ts=message_ts)
        return response["messages"]

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def reply_to_message(
        self, channel_name: str, message_ts: str, reply_text: str
    ) -> bool:
        try:
            self.client.chat_postMessage(
                channel=channel_name, text=reply_text, thread_ts=str(message_ts)
            )
            return True
        except SlackApiError as err:
            if self._handle_send_err(err, channel_name):
                return self.reply_to_message(
                    channel_name=channel_name,
                    message_ts=message_ts,
                    reply_text=reply_text,
                )
            if self.tracking:
                self.tracking.record_internal_exception(err)
            return False

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def send_message(
        self, channel_name: str, message: SlackMessageSchema, **kwargs
    ) -> bool:
        try:
            response = self.client.chat_postMessage(
                channel=channel_name,
                text=message.text,
                blocks=json.dumps(message.blocks) if message.blocks else None,
                attachments=json.dumps(message.attachments)
                if message.attachments
                else None,
            )
            message_ts = response.get("ts", "0")
            self.reply_to_message(
                channel_name=channel_name,
                message_ts=message_ts,
                reply_text=self.unique_id,
            )
            return True
        except SlackApiError as err:
            if self._handle_send_err(err, channel_name):
                return self.send_message(channel_name, message)
            if self.tracking:
                self.tracking.record_internal_exception(err)
            return False


class E2ESlackClient(SlackClient):
    def __init__(self, tracking: Optional[Tracking] = None):
        super().__init__(tracking)

    @staticmethod
    def create_client(
        config: Config, tracking: Optional[Tracking] = None
    ) -> Optional[E2ESlackWebClient]:
        if not config.has_slack:
            return None
        if config.slack_token:
            return E2ESlackWebClient(token=config.slack_token, tracking=tracking)
        elif config.slack_webhook:
            # We can't read Slack messages using webhook
            return None
        return None
