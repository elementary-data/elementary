import json
from abc import ABC, abstractmethod
from typing import Optional

from slack_sdk import WebClient, WebhookClient
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)

OK_STATUS_CODE = 200


class SlackClient(ABC):
    def __init__(
        self, token: str = None, webhook: str = None, tracking: AnonymousTracking = None
    ) -> None:
        self.token = token
        self.webhook = webhook
        self.client = self._initial_client()
        self.tracking = tracking
        self._initial_retry_handlers()
        self.email_to_user_id_cache = {}

    @staticmethod
    def create_client(
        config: Config, tracking: AnonymousTracking = None
    ) -> Optional["SlackClient"]:
        if not config.has_slack:
            return None
        if config.slack_token:
            return SlackWebClient(token=config.slack_token, tracking=tracking)
        elif config.slack_webhook:
            return SlackWebhookClient(webhook=config.slack_webhook, tracking=tracking)

    @abstractmethod
    def _initial_client(self):
        raise NotImplementedError

    def _initial_retry_handlers(self):
        rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=5)
        self.client.retry_handlers.append(rate_limit_handler)

    @abstractmethod
    def send_message(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def send_file(
        self, channel_name: str, file_path: str, message: SlackMessageSchema
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def send_report(self, channel_name: str, report_file_path: str):
        raise NotImplementedError

    @abstractmethod
    def get_user_id_from_email(self, email: str) -> Optional[str]:
        raise NotImplementedError


class SlackWebClient(SlackClient):
    def _initial_client(self):
        return WebClient(token=self.token)

    def send_message(
        self, channel_name: str, message: SlackMessageSchema, **kwargs
    ) -> bool:
        try:
            self.client.chat_postMessage(
                channel=channel_name,
                text=message.text,
                blocks=json.dumps(message.blocks) if message.blocks else None,
                attachments=json.dumps(message.attachments)
                if message.attachments
                else None,
            )
            return True
        except SlackApiError as err:
            if self._handle_send_err(err, channel_name):
                return self.send_message(channel_name, message)
            self.tracking.record_cli_internal_exception(err)
            return False

    def send_file(
        self, channel_name: str, file_path: str, message: SlackMessageSchema
    ) -> bool:
        channel_id = self._get_channel_id(channel_name)
        try:
            self.client.files_upload_v2(
                channel=channel_id,
                initial_comment=message.text,
                file=file_path,
                request_file_info=False,
            )
            return True
        except SlackApiError as err:
            if self._handle_send_err(err, channel_name):
                return self.send_file(channel_name, file_path, message)
            return False

    def send_report(self, channel: str, report_file_path: str):
        send_succeed = self.send_file(
            channel_name=channel,
            file_path=report_file_path,
            message=SlackMessageSchema(text="Elementary monitoring report"),
        )
        if send_succeed:
            logger.info("Sent report to Slack.")
        else:
            logger.error("Failed to send report to Slack.")
        return send_succeed

    def get_user_id_from_email(self, email: str) -> Optional[str]:
        try:
            if email not in self.email_to_user_id_cache:
                user_id = self.client.users_lookupByEmail(email=email)["user"]["id"]
                self.email_to_user_id_cache[email] = user_id
            return self.email_to_user_id_cache[email]
        except SlackApiError as err:
            logger.error(f"Unable to get Slack user ID from email: {err}.")
            return None

    def _get_channel_id(self, channel_name: str) -> Optional[str]:
        cursor = None
        while True:
            response = self.client.conversations_list(
                cursor=cursor,
                types="public_channel,private_channel",
                exclude_archived=True,
                limit=1000,
            )
            for channel in response["channels"]:
                if channel["name"] == channel_name:
                    return channel["id"]
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                return None

    def _join_channel(self, channel_id: str) -> bool:
        try:
            self.client.conversations_join(channel=channel_id)
            return True
        except SlackApiError as e:
            logger.error(f"Elementary app failed to join the given channel. Error: {e}")
            if self.tracking:
                self.tracking.record_cli_internal_exception(e)
            return False

    def _handle_send_err(self, err: SlackApiError, channel_name: str) -> bool:
        err_type = err.response.data["error"]
        if err_type == "not_in_channel":
            logger.info("Elementary app is not in the channel. Attempting to join.")
            channel_id = self._get_channel_id(channel_name)
            if not channel_id:
                return False
            return self._join_channel(channel_id=channel_id)
        elif err_type == "channel_not_found":
            logger.error(
                f"Channel {channel_name} was not found by the Elementary app. Please add the app to the channel."
            )
            return False
        logger.error(
            f"Failed to send a message to channel - {channel_name}. Error: {err}"
        )
        return False


class SlackWebhookClient(SlackClient):
    def _initial_client(self):
        return WebhookClient(
            url=self.webhook, default_headers={"Content-type": "application/json"}
        )

    def send_message(self, message: SlackMessageSchema, **kwargs) -> bool:
        response = self.client.send(
            text=message.text, blocks=message.blocks, attachments=message.attachments
        )
        if response.status_code == OK_STATUS_CODE:
            return True

        else:
            logger.error(
                f"Could not post message to slack via webhook - {self.webhook}. Error: {response.body}"
            )
            return False

    def send_file(self, **kwargs):
        raise NotImplementedError

    def send_report(self, **kwargs):
        raise NotImplementedError

    def get_user_id_from_email(self, email: str) -> Optional[str]:
        return None
