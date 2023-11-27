from typing import Optional

from elementary.config.config import Config
from elementary.monitor.data_monitoring.alerts.integrations.slack.slack import (
    SlackIntegration,
)


class SlackIntegrationMock(SlackIntegration):
    def __init__(
        self,
        override_config_defaults: bool = False,
        is_slack_workflow: bool = False,
        slack_token: Optional[str] = None,
        slack_channel_name: Optional[str] = None,
        slack_webhook: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        config = Config(
            slack_token=slack_token,
            slack_channel_name=slack_channel_name,
            slack_webhook=slack_webhook,
        )
        config.is_slack_workflow = is_slack_workflow
        super().__init__(
            config=config,
            tracking=None,
            override_config_defaults=override_config_defaults,
            *args,
            **kwargs
        )
