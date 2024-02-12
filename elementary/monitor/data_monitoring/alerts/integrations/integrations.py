from typing import Optional

from elementary.config.config import Config
from elementary.exceptions.exceptions import Error
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.slack.slack import (
    SlackIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.teams.teams import (
    TeamsIntegration,
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
    ) -> BaseIntegration:
        if config.has_slack:
            return SlackIntegration(
                config=config,
                tracking=tracking,
                override_config_defaults=override_config_defaults,
            )
        elif config.has_teams:
            return TeamsIntegration(
                config=config,
                tracking=tracking,
                override_config_defaults=override_config_defaults,
            )
        else:
            raise UnsupportedAlertIntegrationError
