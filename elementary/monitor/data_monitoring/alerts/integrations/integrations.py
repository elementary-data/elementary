from typing import Any, Optional, Union, cast

from elementary.config.config import Config
from elementary.exceptions.exceptions import Error
from elementary.messages.messaging_integrations.base_messaging_integration import (
    BaseMessagingIntegration,
    DestinationType,
)
from elementary.messages.messaging_integrations.slack_web import (
    SlackWebMessagingIntegration,
)
from elementary.messages.messaging_integrations.slack_webhook import (
    SlackWebhookMessagingIntegration,
)
from elementary.messages.messaging_integrations.teams_webhook import (
    TeamsWebhookMessagingIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.slack.slack import (
    SlackIntegration,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)


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
    ) -> Union[BaseMessagingIntegration, BaseIntegration]:
        if config.has_slack:
            if config.is_slack_workflow:
                return SlackIntegration(
                    config=config,
                    tracking=tracking,
                )
            if config.slack_token:
                return SlackWebMessagingIntegration.from_token(
                    config.slack_token, tracking
                )
            elif config.slack_webhook:
                return SlackWebhookMessagingIntegration.from_url(
                    config.slack_webhook, tracking
                )
            else:
                raise UnsupportedAlertIntegrationError
        elif config.has_teams:
            return TeamsWebhookMessagingIntegration(config.teams_webhook)
        else:
            raise UnsupportedAlertIntegrationError

    @staticmethod
    def get_destination(
        integration: BaseMessagingIntegration[DestinationType, Any],
        config: Config,
        metadata: dict,
        override_config_defaults: bool = False,
    ) -> DestinationType:
        if (
            isinstance(integration, TeamsWebhookMessagingIntegration)
            and config.has_teams
            and config.teams_webhook
        ):
            return cast(DestinationType, None)
        elif isinstance(integration, SlackWebMessagingIntegration):
            if override_config_defaults:
                if "channel" in metadata:
                    logger.info(
                        f"ignoring channel from metadata: {metadata['channel']}"
                    )
                return config.slack_channel_name
            return metadata.get("channel", config.slack_channel_name)
        elif isinstance(integration, SlackWebhookMessagingIntegration):
            return cast(DestinationType, None)
        raise UnsupportedAlertIntegrationError
