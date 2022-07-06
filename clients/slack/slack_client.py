import json
import logging
from abc import ABC, abstractmethod

from slack_sdk import WebClient, WebhookClient
from slack_sdk.errors import SlackApiError

from clients.slack.schema import SlackMessageSchema

logger = logging.getLogger(__name__)

OK_STATUS_CODE = 200


class SlackClient(ABC):
    def __init__(self, token: str = None, webhook: str = None) -> None:
        self.token = token
        self.webhook = webhook
        self.client = self._initial_client()

    @staticmethod
    def create_slack_client(token: str = None, webhook: str = None):
        if token:
            return SlackWebClient(token=token)
        elif webhook:
            return SlackWebhookClient(webhook=webhook)
        return None

    @abstractmethod
    def _initial_client(self):
        raise NotImplementedError

    @abstractmethod
    def send_message(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, **kwargs):
        raise NotImplementedError


class SlackWebClient(SlackClient):
    def _initial_client(self):
        return WebClient(token=self.token)

    def send_message(self, channel_name: str, message: SlackMessageSchema, **kwargs) -> bool:
        channel_id = self._get_channel_id(channel_name)
        in_channel = self._join_channel(channel_id)
        if not (channel_id and in_channel):
            return False
        try:
            self.client.chat_postMessage(
                channel=channel_id,
                text=message.text,
                blocks=json.dumps(message.blocks) if message.blocks else None,
                attachments=json.dumps(message.attachments) if message.attachments else None
            )
            return True
        except SlackApiError as e:
            logger.error(f"Could not post message to channel - {channel_name}. Error: {e}")
            return False

    def upload_file(self, channel_name: str, file_path: str, message: SlackMessageSchema) -> bool:
        channel_id = self._get_channel_id(channel_name)
        in_channel = self._join_channel(channel_id)
        if not (channel_id and in_channel):
            return False
        try:
            self.client.files_upload(
                channels=channel_id,
                initial_comment=message.text,
                file=file_path
            )
            return True
        except SlackApiError as e:
            logger.error(f"Could not upload the file to the channel - {channel_name}. Error: {e}")
            return False

    def _get_channel_id(self, channel_name: str) -> str:
        try:
            channel = [
                channel
                for channel
                in self.client.conversations_list()["channels"]
                if channel["name"] == channel_name
            ][0]
            return channel["id"]
        except Exception as e:
            logger.error(f"Could not find the channel - {channel_name}. Error: {e}")

    def _join_channel(self, channel_id: str) -> bool:
        try:
            self.client.conversations_join(channel=channel_id)
            return True
        except SlackApiError as e:
            logger.error(f"Elementary app failed to join the given channel. Error: {e}")
            return False


class SlackWebhookClient(SlackClient):
    def _initial_client(self):
        return WebhookClient(url=self.webhook, default_headers={"Content-type": "application/json"})

    def send_message(self, message: SlackMessageSchema, **kwargs) -> bool:
        response = self.client.send(
            text=message.text,
            blocks=message.blocks,
            attachments=message.attachments
        )
        if response.status_code == OK_STATUS_CODE:
            return True

        else:
            logger.error(f"Could not post message to slack via webhook - {self.webhook}. Error: {response.body}")
            return False

    def upload_file(self, **kwargs):
        logger.error(
            f"Slack webhook does not support file uploads. Please use Slack token instead (see documentation on how to configure a slack token)")
