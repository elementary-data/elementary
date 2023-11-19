from typing import Optional

from elementary.cli.e2e.mocks.e2e_slack_client import E2ESlackClient
from elementary.config.config import Config
from elementary.monitor.data_monitoring.alerts.integrations.slack.slack import (
    SlackIntegration,
)
from elementary.tracking.tracking_interface import Tracking


class E2ESlackIntegration(SlackIntegration):
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking] = None,
        override_config_defaults=False,
        *args,
        **kwargs
    ) -> None:
        super().__init__(config, tracking, override_config_defaults, *args, **kwargs)

    def _initial_client(self, *args, **kwargs):
        slack_client = E2ESlackClient.create_client(
            config=self.config, tracking=self.tracking
        )
        if not slack_client:
            raise Exception("Could not initial Slack client")
        return slack_client
