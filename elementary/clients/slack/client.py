import json
import ssl
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union

import certifi
import requests
from ratelimit import limits, sleep_and_retry
from slack_sdk import WebClient, WebhookClient
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler
from slack_sdk.webhook.webhook_response import WebhookResponse

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)

OK_STATUS_CODE = 200
ONE_MINUTE = 60
ONE_SECOND = 1


class SlackClient(ABC):
    def __init__(
        self,
        tracking: Optional[Tracking] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.client = self._initial_client(ssl_context)
        self.tracking = tracking
        self._initial_retry_handlers()
        self.email_to_user_id_cache: Dict[str, str] = {}

    @staticmethod
    def create_client(
        config: Config, tracking: Optional[Tracking] = None
    ) -> Optional["SlackClient"]:
        if not config.has_slack:
            return None
        if config.slack_token:
            logger.debug(
                "Creating Slack client with token (system CA? = %s).",
                config.use_system_ca_files,
            )
            ssl_context = (
                None
                if config.use_system_ca_files
                else ssl.create_default_context(cafile=certifi.where())
            )
            return SlackWebClient(
                token=config.slack_token, tracking=tracking, ssl_context=ssl_context
            )
        elif config.slack_webhook:
            logger.debug(
                "Creating Slack client with webhook (system CA? = %s).",
                config.use_system_ca_files,
            )
            ssl_context = (
                ssl.create_default_context(cafile=certifi.where())
                if not config.use_system_ca_files
                else None
            )
            return SlackWebhookClient(
                webhook=config.slack_webhook,
                is_workflow=config.is_slack_workflow,
                tracking=tracking,
                ssl_context=ssl_context,
            )
        return None

    @abstractmethod
    def _initial_client(self, ssl_context: Optional[ssl.SSLContext]):
        raise NotImplementedError

    def _initial_retry_handlers(self):
        if isinstance(self.client, WebClient):
            rate_limit_handler = RateLimitErrorRetryHandler(max_retry_count=5)
            self.client.retry_handlers.append(rate_limit_handler)

    @abstractmethod
    def send_message(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def send_file(
        self,
        channel_name: str,
        file_path: str,
        message: Optional[SlackMessageSchema] = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    def send_report(self, channel_name: str, report_file_path: str):
        raise NotImplementedError

    @abstractmethod
    def get_user_id_from_email(self, email: str) -> Optional[str]:
        raise NotImplementedError


class SlackWebClient(SlackClient):
    def __init__(
        self,
        token: str,
        tracking: Optional[Tracking] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.token = token
        super().__init__(tracking, ssl_context)

    def _initial_client(self, ssl_context: Optional[ssl.SSLContext]):
        return WebClient(token=self.token, ssl=ssl_context)

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def send_message(
        self, channel_name: str, message: SlackMessageSchema, **kwargs
    ) -> bool:
        try:
            self.client.chat_postMessage(
                channel=channel_name,
                text=message.text,
                blocks=json.dumps(message.blocks) if message.blocks else None,
                attachments=(
                    json.dumps(message.attachments) if message.attachments else None
                ),
            )
            return True
        except SlackApiError as err:
            if self._handle_send_err(err, channel_name):
                return self.send_message(channel_name, message)
            if self.tracking:
                self.tracking.record_internal_exception(err)
            return False

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def send_file(
        self,
        channel_name: str,
        file_path: str,
        message: Optional[SlackMessageSchema] = None,
    ) -> bool:
        channel_id = self._get_channel_id(channel_name)
        try:
            self.client.files_upload_v2(
                channel=channel_id,
                initial_comment=message.text if message else None,
                file=file_path,
                request_file_info=False,
            )
            return True
        except SlackApiError as err:
            if self._handle_send_err(err, channel_name):
                return self.send_file(channel_name, file_path, message)
            return False

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def send_report(self, channel_name: str, report_file_path: str):
        send_succeed = self.send_file(
            channel_name=channel_name, file_path=report_file_path
        )
        if send_succeed:
            logger.info("Sent report to Slack.")
        else:
            logger.error("Failed to send report to Slack.")
        return send_succeed

    @sleep_and_retry
    @limits(calls=50, period=ONE_MINUTE)
    def get_user_id_from_email(self, email: str) -> Optional[str]:
        try:
            if email not in self.email_to_user_id_cache:
                user_id = self.client.users_lookupByEmail(email=email)["user"]["id"]
                self.email_to_user_id_cache[email] = user_id
            return self.email_to_user_id_cache[email]
        except SlackApiError as err:
            logger.error(f"Unable to get Slack user ID from email: {err}.")
            return None

    @sleep_and_retry
    @limits(calls=20, period=ONE_MINUTE)
    def _get_channels(
        self, cursor: Optional[str] = None
    ) -> Tuple[List[dict], Optional[str]]:
        return self.list_conversations(cursor=cursor)

    def list_conversations(
        self, cursor: Optional[str] = None
    ) -> Tuple[List[dict], Optional[str]]:
        response = self.client.conversations_list(
            cursor=cursor,
            types="public_channel,private_channel",
            exclude_archived=True,
            limit=1000,
        )
        channels = response["channels"]
        cursor = response.get("response_metadata", {}).get("next_cursor")
        return channels, cursor

    def _get_channel_id(self, channel_name: str) -> Optional[str]:
        cursor = None
        while True:
            channels, cursor = self._get_channels(cursor)
            for channel in channels:
                if channel["name"] == channel_name:
                    return channel["id"]
            if not cursor:
                return None

    def _join_channel(self, channel_id: str) -> bool:
        try:
            self.client.conversations_join(channel=channel_id)
            logger.info("Elementary app joined the channel successfully.")
            return True
        except SlackApiError as e:
            logger.error(f"Elementary app failed to join the given channel. Error: {e}")
            if self.tracking:
                self.tracking.record_internal_exception(e)
            return False

    def _handle_send_err(self, err: SlackApiError, channel_name: str) -> bool:
        err_type = err.response.data["error"]
        if err_type == "not_in_channel":
            logger.info(
                f'Elementary app is not in the channel "{channel_name}". Attempting to join.'
            )
            channel_id = self._get_channel_id(channel_name)
            if not channel_id:
                logger.info(
                    f'Elementary app could not find the channel "{channel_name}".'
                )
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
    def __init__(
        self,
        webhook: str,
        is_workflow: bool,
        tracking: Optional[Tracking] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.webhook = webhook
        self.is_workflow = is_workflow
        super().__init__(tracking, ssl_context)

    def _initial_client(self, ssl_context: Optional[ssl.SSLContext]):
        if self.is_workflow:
            session = requests.Session()
            if ssl_context is not None:
                # For workflow webhooks, requests.Session doesn't directly support ssl.SSLContext,
                # so we configure it to use certifi's CA bundle instead.
                # The ssl_context parameter indicates that certifi should be used (not system CA files).
                logger.warning(
                    "Workflow webhooks use requests.Session which doesn't fully support SSLContext. "
                    "Using certifi CA bundle for SSL verification instead of --use-system-ca-files setting."
                )
                session.verify = certifi.where()
            # If ssl_context is None, use system CA files (requests default behavior)
            return session

        return WebhookClient(
            url=self.webhook,
            default_headers={"Content-type": "application/json"},
            ssl=ssl_context,
        )

    @sleep_and_retry
    @limits(calls=1, period=ONE_SECOND)
    def send_message(self, message: SlackMessageSchema, **kwargs) -> bool:
        response: Union[requests.Response, WebhookResponse]
        if self.is_workflow:
            # For slack workflows, we need to send the message raw to the webhook
            response = self.client.post(self.webhook, data=message.text)
        else:
            response = self.client.send(
                text=message.text,
                blocks=message.blocks,
                attachments=message.attachments,
            )
        if response.status_code == OK_STATUS_CODE:
            return True
        else:
            response_body = (
                response.text
                if isinstance(response, requests.Response)
                else response.body
            )
            logger.error(
                f"Could not post message to slack via webhook - {self.webhook}. Status code: {response.status_code}, Error: {response_body}"
            )
            return False

    def send_file(
        self,
        channel_name: str,
        file_path: str,
        message: Optional[SlackMessageSchema] = None,
    ) -> bool:
        raise NotImplementedError

    def send_report(self, channel_name: str, report_file_path: str):
        raise NotImplementedError

    def get_user_id_from_email(self, email: str) -> Optional[str]:
        return None
