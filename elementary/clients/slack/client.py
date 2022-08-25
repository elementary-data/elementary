import json
from abc import ABC, abstractmethod
from typing import Optional, List

from slack_sdk import WebClient, WebhookClient
from slack_sdk.errors import SlackApiError

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.utils.log import get_logger

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
        except SlackApiError as err:
            err_type = err.response.data['error']
            if err_type == 'not_in_channel':
                logger.info('Elementary app is not in the channel. Attempting to join.')
                channel_id = self._get_channel_id(channel_name)
                if not channel_id:
                    return False
                joined_successfully = self._join_channel(channel_id=channel_id)
                if not joined_successfully:
                    return False
                self.send_message(channel_name=channel_name, message=message)
            elif err_type == 'channel_not_found':
                logger.error(
                    f'Channel {channel_name} was not found by the Elementary app. Please add the app to the channel.')
                return False
            logger.error(f"Could not post message to channel - {channel_name}. Error: {err}")
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

    def send_report(self, channel: str, report_file_path: str):
        send_succeed = self.send_file(
            channel_name=channel,
            file_path=report_file_path,
            message=SlackMessageSchema(text="Elementary monitoring report")
        )
        if send_succeed:
            logger.info('Sent report to Slack.')
        else:
            logger.error('Failed to send report to Slack.')
        return send_succeed

    def _get_channel_id(self, channel_name: str) -> Optional[str]:
        try:
            available_channels = self._get_channels()
            available_channel_names = []
            for available_channel in available_channels:
                available_channel_name = available_channel['name']
                available_channel_names.append(available_channel_name)
                if available_channel_name == channel_name:
                    return available_channel['id']
            logger.error(f'Channel {channel_name} not found. Available channels: {available_channel_names}')
            return None
        except Exception:
            logger.error(f'Elementary app failed to query Slack channels.')
            return None

    def _join_channel(self, channel_id: str) -> bool:
        try:
            self.client.conversations_join(channel=channel_id)
            return True
        except SlackApiError as e:
            logger.error(f"Elementary app failed to join the given channel. Error: {e}")
            return False

    def _get_channels(self) -> List[dict]:
        try:
            channels = []
            has_more = True
            cursor = None
            while has_more:
                response = self.client.conversations_list(cursor=cursor, types='public_channel,private_channel',
                                                          exclude_archived=True)
                channels.extend(response["channels"])
                cursor = response.get("response_metadata", {}).get("next_cursor")
                has_more = True if cursor else False
            return channels
        except SlackApiError as e:
            logger.error(f"Elementary app failed to query all Slack public channels. Error: {e}")
            return []


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
