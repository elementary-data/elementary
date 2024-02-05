from abc import ABC, abstractmethod
from typing import Optional

from pymsteams import cardsection, connectorcard, potentialaction  # type: ignore
from retry import retry

from elementary.config.config import Config
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)

OK_STATUS_CODE = 200


class TeamsClient(ABC):
    def __init__(
        self,
        tracking: Optional[Tracking] = None,
    ):
        self.client = self._initial_client()
        self.tracking = tracking

    @staticmethod
    def create_client(
        config: Config, tracking: Optional[Tracking] = None
    ) -> Optional["TeamsClient"]:
        if not config.has_teams:
            return None
        if config.teams_webhook:
            return TeamsWebhookMessageBuilderClient(
                webhook=config.teams_webhook, tracking=tracking
            )
        return None

    @abstractmethod
    def _initial_client(self):
        raise NotImplementedError

    def _initial_retry_handlers(self):
        raise NotImplementedError

    @abstractmethod
    def send_message(self, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def title(self, title: str):
        raise NotImplementedError

    @abstractmethod
    def text(self, text: str):
        raise NotImplementedError

    @abstractmethod
    def addSection(self, section: cardsection):
        raise NotImplementedError

    @abstractmethod
    def addPotentialAction(self, action: potentialaction):
        raise NotImplementedError


class TeamsWebhookClient(TeamsClient):
    def __init__(
        self,
        webhook: str,
        tracking: Optional[Tracking] = None,
    ):
        self.webhook = webhook
        super().__init__(tracking)

    def _initial_client(self):
        return connectorcard(self.webhook)

    @retry(tries=3, delay=1, backoff=2, max_delay=5)
    def send_message(self, **kwargs) -> bool:
        self.client.send()
        response = self.client.last_http_response

        if response.status_code == OK_STATUS_CODE:
            return True
        else:
            logger.error(
                "Could not post message to teams via webhook - %s. Error: %s",
                {self.webhook},
                {response.body},
            )
        return False


class TeamsWebhookMessageBuilderClient(TeamsWebhookClient):
    def __init__(
        self,
        webhook: str,
        tracking: Optional[Tracking] = None,
    ):
        super().__init__(webhook, tracking)

    def title(self, title: str):
        self.client.title(title)

    def text(self, text: str):
        self.client.text(text)

    def addSection(self, section: cardsection):
        self.client.addSection(section)

    def addPotentialAction(self, action: potentialaction):
        self.client.addPotentialAction(action)
