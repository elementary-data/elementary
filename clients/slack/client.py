import json
from abc import ABC, abstractmethod
from typing import Optional

from slack_sdk import WebClient, WebhookClient
from slack_sdk.errors import SlackApiError

from clients.slack.schema import SlackMessageSchema
from config.config import Config
from utils.log import get_logger

logger = get_logger(__name__)

OK_STATUS_CODE = 200


class SlackClient(ABC):
    def __init__(self, token: str = None, webhook: str = None) -> None:
        self.token = token
        self.webhook = webhook
        self.client = self._initial_client()

    @staticmethod
    def create_client(config: Config) -> Optional['SlackClient']:
        if not config.has_slack:
            return None
        if config.slack_token:
            return SlackWebClient(token=config.slack_token)
        elif config.slack_webhook:
            return SlackWebhookClient(webhook=config.slack_webhook)

    @abstractmethod
    def _initial_client(self):
        raise NotImplementedError

    @abstractmethod
    def send_message(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def send_file(self, channel_name: str, file_path: str, message: SlackMessageSchema) -> bool:
        raise NotImplementedError

    @abstractmethod
    def send_report(self, channel_name: str, report_file_path: str):
        raise NotImplementedError


class SlackWebClient(SlackClient):
    def _initial_client(self):
        return WebClient(token=self.token)

    def send_message(self, channel_name: str, message: SlackMessageSchema, **kwargs) -> bool:
        try:
            self.client.chat_postMessage(
                channel=channel_name,
                text=message.text,
                blocks=json.dumps(message.blocks) if message.blocks else None,
                attachments=json.dumps(message.attachments) if message.attachments else None
            )
            return True
        except SlackApiError as e:
            logger.error(f"Could not post message to channel - {channel_name}. Error: {e}")
            return False

    def send_file(self, channel_name: str, file_path: str, message: SlackMessageSchema) -> bool:
        try:
            self.client.files_upload(
                channels=channel_name,
                initial_comment=message.text,
                file=file_path
            )
            return True
        except SlackApiError as e:
            logger.error(f"Could not upload the file to the channel - {channel_name}. Error: {e}")
            return False

    def send_report(self, channel_name: str, report_file_path: str):
        send_succeed = self.send_file(
            channel_name=channel_name,
            file_path=report_file_path,
            message=SlackMessageSchema(text="Elementary monitoring report")
        )
        if send_succeed:
            logger.info('Sent report to Slack.')
        else:
            logger.error('Failed to send report to Slack.')
        return send_succeed


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

    def send_file(self, **kwargs):
        logger.error(
            f"Slack webhook does not support sending files. Please use Slack token instead (see documentation on how to configure a slack token)")

    def send_report(self, **kwargs):
        logger.error(
            f"Slack webhook does not support sending reports. Please use Slack token instead (see documentation on how to configure a slack token)")
