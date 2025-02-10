from typing import Any, Optional, Union

from elementary.config.config import Config
from elementary.exceptions.exceptions import Error
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
)
from elementary.messages.messaging_integrations.teams_webhook import (
    ChannelWebhook,
    TeamsWebhookMessagingIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.slack.slack import (
    SlackIntegration,
)
from elementary.tracking.tracking_interface import Tracking


class UnsupportedAlertIntegrationError(Error):
    """Exception raised while executing edr monitor without a supported integration"""

    def __init__(self) -> None:
        super().__init__(
            "Failed to run edr monitor - no supported integration was provided"
        )


class Integrations:
    @staticmethod
    def get_integration(
        config: Config,
        tracking: Optional[Tracking] = None,
        override_config_defaults: bool = False,
    ) -> Union[BaseIntegration, BaseMessagingIntegration]:
        # Factory method that returns either a legacy BaseIntegration or new BaseMessagingIntegration
        # This allows for a gradual migration from the old integration system to the new messaging system
        # - Slack currently uses the legacy BaseIntegration
        # - Teams uses the new BaseMessagingIntegration
        if config.has_slack:
            return SlackIntegration(
                config=config,
                tracking=tracking,
                override_config_defaults=override_config_defaults,
            )
        elif config.has_teams:
            return TeamsWebhookMessagingIntegration()
        else:
            raise UnsupportedAlertIntegrationError

    @staticmethod
    def get_destination(integration: BaseMessagingIntegration, config: Config) -> Any:
        # Helper method to get the appropriate destination for BaseMessagingIntegration implementations
        # Each messaging integration type may have different destination requirements
        # Currently supports Teams webhook destinations
        if (
            isinstance(integration, TeamsWebhookMessagingIntegration)
            and config.has_teams
            and config.teams_webhook
        ):
            return ChannelWebhook(webhook=config.teams_webhook)
        return None
