from collections import defaultdict
from typing import List, Dict, Optional

from ratelimit import sleep_and_retry, limits

from elementary.clients.slack.client import SlackWebhookClient, ONE_MINUTE, SlackWebClient
from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.tracking.anonymous_tracking import AnonymousTracking


class SlackWebClientMock(SlackWebClient):
    """
    Mock the Slack Web Client, saving sent messages and files internally instead of sending them anywhere.
    """

    def __init__(
            self, token: str = None, webhook: str = None, tracking: AnonymousTracking = None
    ) -> None:
        self.sent_messages: Dict[str, List[SlackMessageSchema]] = defaultdict(
            lambda: [])  # map channel to a list that holds sent messages
        self.sent_files: Dict[str, List] = defaultdict(
            lambda: [])  # map channel to a list that holds sent file_paths + possibly messages.
        super().__init__(token, webhook, tracking=None)

    def send_message(
            self, channel_name: str, message: SlackMessageSchema, **kwargs
    ) -> bool:
        self.sent_messages[channel_name].append(message)
        return True

    def send_file(
            self,
            channel_name: str,
            file_path: str,
            message: Optional[SlackMessageSchema] = None,
    ) -> bool:
        self.sent_files[channel_name].append((file_path, message))
        return True
    def get_user_id_from_email(self, email:str)-> str:
        return email

class SlackWebhookClientMock(SlackWebhookClient):
    def __init__(
            self, token: str = None, webhook: str = None, tracking: AnonymousTracking = None
    ) -> None:
        self.sent_messages: List[SlackMessageSchema] = []  # List of alerts that were sent.
        super().__init__(token=token, webhook=webhook, tracking=None)

    @sleep_and_retry
    @limits(calls=50, period=ONE_MINUTE)
    def send_message(self, message: SlackMessageSchema, **kwargs) -> bool:
        self.sent_messages.append(message)
        return True

    def get_user_id_from_email(self, email:str)-> str:
        return email