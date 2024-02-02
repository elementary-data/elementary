from datetime import datetime
from typing import Dict, Optional, Union

from pymsteams import cardsection

from elementary.clients.teams.client import TeamsClient
from elementary.config.config import Config
from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    get_model_runs_link,
    get_test_runs_link,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger
from elementary.utils.strings import prettify_and_dedup_list

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
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"

        if alert.suppression_interval:
            title = "\n".join(
                [
                    title,
                    f"Test: {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                ]
            )
            title = "\n".join([title, f"Status: {alert.status}"])
            title = "\n".join([title, f"Time: {alert.detected_at_str}     |"])
            title = "\n".join(
                [title, f"Suppression interval:* {alert.suppression_interval} hours"]
            )
        else:
            title = "\n".join(
                [
                    title,
                    f"Test: {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                ]
            )
            title = "\n".join([title, f"Status: {alert.status}     |"])
            title = "\n".join([title, f"{alert.detected_at_str}"])

        test_runs_report_link = get_test_runs_link(
            alert.report_url, alert.elementary_unique_id
        )
        if test_runs_report_link:
            title = "\n".join(
                [title, f"<{test_runs_report_link.url}|{test_runs_report_link.text}>"]
            )

        self.client.title(title)
        # This is required by pymsteams..
        self.client.text("**Elementary generated this message**")

        if TABLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Table*")
            section.activityText(f"_{alert.table_full_name}_")
            self.client.addSection(section)

        if COLUMN_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Column*")
            section.activityText(f'_{alert.column_name or "No column"}_')
            self.client.addSection(section)

        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = prettify_and_dedup_list(alert.tags or [])
            section = cardsection()
            section.activityTitle("*Tags*")
            section.activityText(f'_{tags or "No tags"}_')
            self.client.addSection(section)

        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            section = cardsection()
            section.activityTitle("*Owners*")
            section.activityText(f'_{owners or "No owners"}_')
            self.client.addSection(section)

        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
            section = cardsection()
            section.activityTitle("*Subscribers*")
            section.activityText(f'_{subscribers or "No subscribers"}_')
            self.client.addSection(section)

        if DESCRIPTION_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Description*")
            section.activityText(f'_{alert.test_description or "No description"}_')
            self.client.addSection(section)

        if (
            RESULT_MESSAGE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.error_message
        ):
            section = cardsection()
            section.activityTitle("*Result message*")
            section.activityText(f"_{alert.error_message.strip()}_")
            self.client.addSection(section)

        if (
            TEST_RESULTS_SAMPLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_rows_sample
        ):
            section = cardsection()
            section.activityTitle("*Test results sample*")
            section.activityText(f"```{alert.test_rows_sample}```")
            self.client.addSection(section)

        # This lacks logic to handle the case where the message is too long
        if (
            TEST_QUERY_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_results_query
        ):
            section = cardsection()
            section.activityTitle("*Test query*")
            section.activityText(f"{alert.test_results_query}")
            self.client.addSection(section)

        if (
            TEST_PARAMS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_params
        ):
            section = cardsection()
            section.activityTitle("*Test parameters*")
            section.activityText(f"```{alert.test_params}```")
            self.client.addSection(section)

    def _get_elementary_test_template(self, alert: TestAlertModel, *args, **kwargs):
        self.client.title(f"{self._get_display_name(alert.status)}: {alert.summary}")
        self.client.text(f"{self._get_display_name(alert.status)}: {alert.summary}")

        anomalous_value = (
            alert.other if alert.test_type == "anomaly_detection" else None
        )

        title = ""
        if alert.test_type == "schema_change":
            title = f"{alert.summary}"
        else:
            title = f"{self._get_display_name(alert.status)}: {alert.summary}"

        if alert.suppression_interval:
            title = "\n".join(
                [
                    title,
                    f"*Test:* {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                ]
            )
            title = "\n".join([title, f"Status: {alert.status}"])
            title = "\n".join([title, f"Time: {alert.detected_at_str}     |"])
            title = "\n".join(
                [title, f"Suppression interval:* {alert.suppression_interval} hours"]
            )
        else:
            title = "\n".join(
                [
                    title,
                    f"Test: {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                ]
            )
            title = "\n".join([title, f"Status: {alert.status}     |"])
            title = "\n".join([title, f"{alert.detected_at_str}"])

        test_runs_report_link = get_test_runs_link(
            alert.report_url, alert.elementary_unique_id
        )
        if test_runs_report_link:
            title = "\n".join(
                [title, f"<{test_runs_report_link.url}|{test_runs_report_link.text}>"]
            )

        self.client.title(title)
        # This is required by pymsteams..
        self.client.text("**Elementary generated this message**")

        if TABLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Table*")
            section.activityText(f"_{alert.table_full_name}_")
            self.client.addSection(section)

        if COLUMN_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Column*")
            section.activityText(f'_{alert.column_name or "No column"}_')
            self.client.addSection(section)

        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = prettify_and_dedup_list(alert.tags or [])
            section = cardsection()
            section.activityTitle("*Tags*")
            section.activityText(f'_{tags or "No tags"}_')
            self.client.addSection(section)

        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            section = cardsection()
            section.activityTitle("*Owners*")
            section.activityText(f'_{owners or "No owners"}_')
            self.client.addSection(section)

        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
            section = cardsection()
            section.activityTitle("*Subscribers*")
            section.activityText(f'_{subscribers or "No subscribers"}_')
            self.client.addSection(section)

        if DESCRIPTION_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Description*")
            section.activityText(f'_{alert.test_description or "No description"}_')
            self.client.addSection(section)

        if (
            RESULT_MESSAGE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.error_message
        ):
            section = cardsection()
            section.activityTitle("*Result message*")
            section.activityText(f"```{alert.error_message.strip()}```")
            self.client.addSection(section)

        if (
            TEST_RESULTS_SAMPLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and anomalous_value
        ):
            section = cardsection()
            section.activityTitle("*Test results sample*")
            message = ""
            if alert.column_name:
                message = f"*Column*: {alert.column_name}     |     *Anomalous Values*: {anomalous_value}"
            else:
                message = f"*Anomalous Values*: {anomalous_value}"
            section.activityText(message)
            self.client.addSection(section)

        if (
            TEST_PARAMS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_params
        ):
            section = cardsection()
            section.activityTitle("*Test parameters*")
            section.activityText(f"```{alert.test_params}```")
            self.client.addSection(section)

    def _get_model_template(self, alert: ModelAlertModel, *args, **kwargs):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"

        if alert.suppression_interval:
            title = "\n".join([title, f"*Model:* {alert.alias}     |"])
            title = "\n".join([title, f"Status: {alert.status}"])
            title = "\n".join([title, f"Time: {alert.detected_at_str}     |"])
            title = "\n".join(
                [title, f"Suppression interval:* {alert.suppression_interval} hours"]
            )
        else:
            title = "\n".join(
                [
                    title,
                    f"*Model:* {alert.alias}     |",
                ]
            )
            title = "\n".join([title, f"Status: {alert.status}     |"])
            title = "\n".join([title, f"{alert.detected_at_str}"])

        model_runs_report_link = get_model_runs_link(
            alert.report_url, alert.model_unique_id
        )
        if model_runs_report_link:
            title = "\n".join(
                [title, f"<{model_runs_report_link.url}|{model_runs_report_link.text}>"]
            )

        self.client.title(title)
        # This is required by pymsteams..
        self.client.text("**Elementary generated this message**")

        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = prettify_and_dedup_list(alert.tags or [])
            section = cardsection()
            section.activityTitle("*Tags*")
            section.activityText(f'_{tags or "No tags"}_')
            self.client.addSection(section)

        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            section = cardsection()
            section.activityTitle("*Owners*")
            section.activityText(f'_{owners or "No owners"}_')
            self.client.addSection(section)

        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
            section = cardsection()
            section.activityTitle("*Subscribers*")
            section.activityText(f'_{subscribers or "No subscribers"}_')
            self.client.addSection(section)

        if (
            RESULT_MESSAGE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.message
        ):
            section = cardsection()
            section.activityTitle("*Result message*")
            section.activityText(f"```{alert.message.strip()}```")
            self.client.addSection(section)

        if alert.materialization:
            section = cardsection()
            section.activityTitle("*Materialization*")
            section.activityText(f"`{str(alert.materialization)}`")
            self.client.addSection(section)
        if alert.full_refresh:
            section = cardsection()
            section.activityTitle("*Full refresh*")
            section.activityText(f"`{alert.full_refresh}`")
            self.client.addSection(section)
        if alert.path:
            section = cardsection()
            section.activityTitle("*Path*")
            section.activityText(f"`{alert.path}`")
            self.client.addSection(section)

    def _get_snapshot_template(self, alert: ModelAlertModel, *args, **kwargs):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"

        if alert.suppression_interval:
            title = "\n".join([title, f"*Snapshot:* {alert.alias}     |"])
            title = "\n".join([title, f"Status: {alert.status}"])
            title = "\n".join([title, f"Time: {alert.detected_at_str}     |"])
            title = "\n".join(
                [title, f"Suppression interval:* {alert.suppression_interval} hours"]
            )
        else:
            title = "\n".join(
                [
                    title,
                    f"*Snapshot:* {alert.alias}     |",
                ]
            )
            title = "\n".join([title, f"Status: {alert.status}     |"])
            title = "\n".join([title, f"{alert.detected_at_str}"])

        model_runs_report_link = get_model_runs_link(
            alert.report_url, alert.model_unique_id
        )
        if model_runs_report_link:
            title = "\n".join(
                [title, f"<{model_runs_report_link.url}|{model_runs_report_link.text}>"]
            )

        self.client.title(title)
        # This is required by pymsteams..
        self.client.text("**Elementary generated this message**")

        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = prettify_and_dedup_list(alert.tags or [])
            section = cardsection()
            section.activityTitle("*Tags*")
            section.activityText(f'_{tags or "No tags"}_')
            self.client.addSection(section)

        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            section = cardsection()
            section.activityTitle("*Owners*")
            section.activityText(f'_{owners or "No owners"}_')
            self.client.addSection(section)

        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
            section = cardsection()
            section.activityTitle("*Subscribers*")
            section.activityText(f'_{subscribers or "No subscribers"}_')
            self.client.addSection(section)

        if alert.message:
            section = cardsection()
            section.activityTitle("*Result message*")
            section.activityText(f"```{alert.message.strip()}```")
            self.client.addSection(section)

        if alert.original_path:
            section = cardsection()
            section.activityTitle("*Path*")
            section.activityText(f"`{alert.original_path}`")
            self.client.addSection(section)

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
