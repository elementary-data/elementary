from typing import Optional

from elementary.cli.e2e.mocks.e2e_slack_integration import E2ESlackIntegration
from elementary.config.config import Config
from elementary.monitor.data_monitoring.alerts.data_monitoring_alerts import (
    DataMonitoringAlerts,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class E2EDataMonitoringAlerts(DataMonitoringAlerts):
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking] = None,
        filter: Optional[str] = None,
        force_update_dbt_package: bool = False,
        disable_samples: bool = False,
        send_test_message_on_success: bool = False,
        global_suppression_interval: int = 0,
        override_config: bool = False,
    ):
        tracking = None
        super().__init__(
            config,
            tracking,
            filter,
            force_update_dbt_package,
            disable_samples,
            send_test_message_on_success,
            global_suppression_interval,
            override_config,
        )

    def _get_integration_client(self):
        return E2ESlackIntegration(
            config=self.config,
            tracking=self.tracking,
            override_config_defaults=self.override_config_defaults,
        )

    # Validate that we actually posted the alerts at Slack
    # Currently only checking that we sent the right amount of alerts
    def validate_send_alerts(self):
        logger.info("Validating alerts sent successfully")
        validated_alerts = 0

        integration_instance_unique_id = self.alerts_integraion.client.unique_id
        channel_messages = (
            self.alerts_integraion.client.get_channel_messages_with_replies(
                channel_name=self.config.slack_channel_name, after_hours=0.5
            )
        )
        for messages in channel_messages:
            if len(messages) == 2:
                if messages[1].get("text") == integration_instance_unique_id:
                    validated_alerts += 1

        validation_passed = validated_alerts == self.alerts_to_send_count
        if validation_passed:
            logger.info("Validation passed - all of the alerts were sent successfully")
        else:
            logger.error(
                f"Validation fails - expected {self.alerts_to_send_count} to be sent, but found only {validated_alerts}."
            )
        return validation_passed
