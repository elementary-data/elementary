from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from elementary.clients.teams.client import TeamsClient
from elementary.config.config import Config
from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)

TABLE_FIELD = "table"
COLUMN_FIELD = "column"
DESCRIPTION_FIELD = "description"
OWNERS_FIELD = "owners"
TAGS_FIELD = "tags"
SUBSCRIBERS_FIELD = "subscribers"
RESULT_MESSAGE_FIELD = "result_message"
TEST_PARAMS_FIELD = "test_parameters"
TEST_QUERY_FIELD = "test_query"
TEST_RESULTS_SAMPLE_FIELD = "test_results_sample"
DEFAULT_ALERT_FIELDS = [
    TABLE_FIELD,
    COLUMN_FIELD,
    DESCRIPTION_FIELD,
    OWNERS_FIELD,
    TAGS_FIELD,
    SUBSCRIBERS_FIELD,
    RESULT_MESSAGE_FIELD,
    TEST_PARAMS_FIELD,
    TEST_QUERY_FIELD,
    TEST_RESULTS_SAMPLE_FIELD,
]

STATUS_DISPLAYS: Dict[str, Dict] = {
    "fail": {"display_name": "Failure"},
    "warn": {"display_name": "Warning"},
    "error": {"display_name": "Error"},
}


class TeamsIntegration(BaseIntegration):
    def __init__(
        self,
        config: Config,
        tracking: Optional[Tracking] = None,
        override_config_defaults=False,
        *args,
        **kwargs,
    ) -> None:
        self.config = config
        self.tracking = tracking
        self.override_config_defaults = override_config_defaults
        self.message_builder = None
        super().__init__()

        # Enforce typing
        self.client: TeamsClient

    def _initial_client(self, *args, **kwargs) -> TeamsClient:
        teams_client = TeamsClient.create_client(
            config=self.config, tracking=self.tracking
        )
        if not teams_client:
            raise Exception("Could not create a Teams client")
        return teams_client

    def _get_dbt_test_template(self, alert: TestAlertModel, *args, **kwargs):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")
        self.client.text(f"{self._get_display_name(alert.status)}: {alert.summary}")

    def _get_elementary_test_template(self, alert: TestAlertModel, *args, **kwargs):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")
        self.client.text(f"{self._get_display_name(alert.status)}: {alert.summary}")

    def _get_model_template(self, alert: ModelAlertModel, *args, **kwargs):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")
        self.client.text(f"{self._get_display_name(alert.status)}: {alert.summary}")

    def _get_snapshot_template(self, alert: ModelAlertModel, *args, **kwargs):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")
        self.client.text(f"{self._get_display_name(alert.status)}: {alert.summary}")

    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, *args, **kwargs
    ):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")
        self.client.text(f"{self._get_display_name(alert.status)}: {alert.summary}")

    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, *args, **kwargs
    ):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")
        self.client.text(f"{self._get_display_name(alert.status)}: {alert.summary}")

    def _get_fallback_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs,
    ):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")

    def _get_test_message_template(self, *args, **kwargs):
        self.client.title(
            f"Elementary monitor ran successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        self.client.text(
            f"Elementary monitor ran successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )

    def send_alert(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs,
    ) -> bool:
        try:
            self._get_alert_template(alert)
            sent_successfully = self.client.send_message()
        except Exception as e:
            logger.error(e)
            sent_successfully = False

        if not sent_successfully:
            try:
                self._get_fallback_template(alert)
                fallback_sent_successfully = self.client.send_message()
            except Exception:
                fallback_sent_successfully = False
            return fallback_sent_successfully
        return sent_successfully

    @staticmethod
    def _get_display_name(alert_status: Optional[str]) -> str:
        if alert_status is None:
            return "Unknown"
        return STATUS_DISPLAYS.get(alert_status, {}).get("display_name", alert_status)

    def send_test_message(self, *args, **kwargs) -> bool:
        self._get_test_message_template()
