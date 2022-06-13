import logging
import json
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackClient:
    def __init__(self, slack_token: str) -> None:
        self.slack_token = slack_token
        self.client = SlackClient._initial_client(self.slack_token)

    @staticmethod
    def _initial_client(token: str) -> WebClient:
        return WebClient(token=token)

    def send_message(self, channel_name: str, message: str = None, attachments: list = None) -> None:
        channel_id = self._get_channel_id(channel_name)
        in_channel = self._join_channel(channel_id)
        if not (channel_id and in_channel):
            return 
        try:
            self.client.chat_postMessage(
                channel=channel_id,
                text=message,
                attachments=json.dumps(attachments)
            )
        except SlackApiError as e:
            logger.error(f"Could not post message to channel - {channel_name}. Error: {e}")
    
    def upload_file(self, channel_name: str, file_path: str, message: str = None) -> None:
        channel_id = self._get_channel_id(channel_name)
        in_channel = self._join_channel(channel_id)
        if not (channel_id and in_channel):
            return 
        try:
            self.client.files_upload(
                channels=channel_id,
                initial_comment=message,
                file=file_path
            )
        except SlackApiError as e:
            logger.error(f"Could not upload the file to the channel - {channel_name}. Error: {e}")   

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
