import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import pandas as pd
from pymsteams import cardsection, potentialaction  # type: ignore

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
    ReportLinkData,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.json_utils import (
    list_of_lists_of_strings_to_comma_delimited_unique_strings,
)
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

    @staticmethod
    def _get_alert_sub_title(
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
        ],
    ) -> str:
        subtitle = "**"
        subtitle += f"Status: {alert.status}"
        if alert.suppression_interval:
            subtitle += f"   |   Time: {alert.detected_at_str}"
            subtitle += (
                f"   |   Suppression interval: {alert.suppression_interval} hours"
            )
        else:
            subtitle += f"   |   {alert.detected_at_str}"
        subtitle += "**"

        return subtitle

    @staticmethod
    def _get_potential_action(reportlink: ReportLinkData):
        action = potentialaction(reportlink.text)
        action.addOpenURI(
            reportlink.text,
            [{"os": "default", "uri": reportlink.url}],
        )
        return action

    def _add_report_link_if_applicable(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
    ):
        report_link = alert.get_report_link()
        if report_link:
            action = self._get_potential_action(report_link)
            self.client.addPotentialAction(action)

    def _add_table_field_section_if_applicable(self, alert: TestAlertModel):
        if TABLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Table*")
            section.activityText(f"_{alert.table_full_name}_")
            self.client.addSection(section)

    def _add_column_field_section_if_applicable(self, alert: TestAlertModel):
        if COLUMN_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Column*")
            section.activityText(f'_{alert.column_name or "No column"}_')
            self.client.addSection(section)

    def _add_tags_field_section_if_applicable(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
        ],
    ):
        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = prettify_and_dedup_list(alert.tags or [])
            section = cardsection()
            section.activityTitle("*Tags*")
            section.activityText(f'_{tags or "No tags"}_')
            self.client.addSection(section)

    def _add_owners_field_section_if_applicable(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
        ],
    ):
        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            section = cardsection()
            section.activityTitle("*Owners*")
            section.activityText(f'_{owners or "No owners"}_')
            self.client.addSection(section)

    def _add_subscribers_field_section_if_applicable(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
        ],
    ):
        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
            section = cardsection()
            section.activityTitle("*Subscribers*")
            section.activityText(f'_{subscribers or "No subscribers"}_')
            self.client.addSection(section)

    def _add_description_field_section_if_applicable(self, alert: TestAlertModel):
        if DESCRIPTION_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Description*")
            section.activityText(f'_{alert.test_description or "No description"}_')
            self.client.addSection(section)

    def _add_result_message_field_section_if_applicable(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
        ],
    ):
        message = None
        if RESULT_MESSAGE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            section = cardsection()
            section.activityTitle("*Result message*")
            if isinstance(alert, ModelAlertModel):
                if alert.message:
                    message = alert.message.strip()
            elif isinstance(alert, TestAlertModel):
                if alert.error_message:
                    message = alert.error_message.strip()
            if not message:
                message = "No result message"
            section.activityText(f"_{message}_")
            self.client.addSection(section)

    def _add_test_query_field_section_if_applicable(self, alert: TestAlertModel):
        # This lacks logic to handle the case where the message is too long
        if (
            TEST_QUERY_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_results_query
        ):
            section = cardsection()
            section.activityTitle("*Test query*")
            section.activityText(f"```{alert.test_results_query.strip()}")
            self.client.addSection(section)

    def _add_test_params_field_section_if_applicable(self, alert: TestAlertModel):
        if (
            TEST_PARAMS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_params
        ):
            section = cardsection()
            section.activityTitle("*Test parameters*")
            section.activityText(f"```{alert.test_params}```")
            self.client.addSection(section)

    def _add_test_results_sample_field_section_if_applicable(
        self, alert: TestAlertModel
    ):
        if TEST_RESULTS_SAMPLE_FIELD in (
            alert.alert_fields or DEFAULT_ALERT_FIELDS
        ) and (alert.test_rows_sample or alert.test_type == "anomaly_detection"):
            section = cardsection()
            section.activityTitle("*Test results sample*")
            if alert.test_type == "anomaly_detection":
                anomalous_value = alert.other
                if alert.column_name:
                    message = f"*Column*: {alert.column_name}     |     *Anomalous Values*: {anomalous_value}"
                else:
                    message = f"*Anomalous Values*: {anomalous_value}"
            else:
                df = pd.DataFrame(alert.test_rows_sample)
                message = df.to_markdown(index=False)
            section.activityText(message)
            self.client.addSection(section)

    def _get_dbt_test_template(self, alert: TestAlertModel, *args, **kwargs):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)

        self._add_report_link_if_applicable(alert)

        self.client.title(title)
        self.client.text(subtitle)

        self._add_table_field_section_if_applicable(alert)
        self._add_column_field_section_if_applicable(alert)
        self._add_tags_field_section_if_applicable(alert)
        self._add_owners_field_section_if_applicable(alert)
        self._add_subscribers_field_section_if_applicable(alert)
        self._add_description_field_section_if_applicable(alert)
        self._add_result_message_field_section_if_applicable(alert)
        self._add_test_results_sample_field_section_if_applicable(alert)
        self._add_test_query_field_section_if_applicable(alert)
        self._add_test_params_field_section_if_applicable(alert)

    def _get_elementary_test_template(self, alert: TestAlertModel, *args, **kwargs):
        if alert.test_type == "schema_change":
            title = f"{alert.summary}"
        else:
            title = f"{self._get_display_name(alert.status)}: {alert.summary}"

        subtitle = self._get_alert_sub_title(alert)

        self._add_report_link_if_applicable(alert)

        self.client.title(title)
        self.client.text(subtitle)

        self._add_table_field_section_if_applicable(alert)
        self._add_column_field_section_if_applicable(alert)
        self._add_tags_field_section_if_applicable(alert)
        self._add_owners_field_section_if_applicable(alert)
        self._add_subscribers_field_section_if_applicable(alert)
        self._add_description_field_section_if_applicable(alert)
        self._add_result_message_field_section_if_applicable(alert)
        self._add_test_results_sample_field_section_if_applicable(alert)
        self._add_test_params_field_section_if_applicable(alert)

    def _get_model_template(self, alert: ModelAlertModel, *args, **kwargs):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)

        self._add_report_link_if_applicable(alert)

        self.client.title(title)
        self.client.text(subtitle)
        self._add_tags_field_section_if_applicable(alert)
        self._add_owners_field_section_if_applicable(alert)
        self._add_subscribers_field_section_if_applicable(alert)
        self._add_result_message_field_section_if_applicable(alert)

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
        subtitle = self._get_alert_sub_title(alert)

        self._add_report_link_if_applicable(alert)

        self.client.title(title)
        self.client.text(subtitle)

        self._add_tags_field_section_if_applicable(alert)
        self._add_owners_field_section_if_applicable(alert)
        self._add_subscribers_field_section_if_applicable(alert)
        self._add_result_message_field_section_if_applicable(alert)

        if alert.original_path:
            section = cardsection()
            section.activityTitle("*Path*")
            section.activityText(f"`{alert.original_path}`")
            self.client.addSection(section)

    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, *args, **kwargs
    ):
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = self._get_alert_sub_title(alert)

        self._add_report_link_if_applicable(alert)

        self.client.title(title)
        self.client.text(subtitle)

        self._add_tags_field_section_if_applicable(alert)
        self._add_owners_field_section_if_applicable(alert)
        self._add_subscribers_field_section_if_applicable(alert)

        if alert.freshness_description:
            section = cardsection()
            section.activityTitle("*Description*")
            section.activityText(f'_{alert.freshness_description or "No description"}_')
            self.client.addSection(section)

        if alert.status == "runtime error":
            section = cardsection()
            section.activityTitle("*Result message*")
            section.activityText(
                f"Failed to calculate the source freshness\n```{alert.error}```"
            )
            self.client.addSection(section)
        else:
            section = cardsection()
            section.activityTitle("*Result message*")
            section.activityText(f"```{alert.result_description}```")
            self.client.addSection(section)

        if alert.status != "runtime error":
            section = cardsection()
            section.activityTitle("*Time Elapsed*")
            section.activityText(
                f"{timedelta(seconds=alert.max_loaded_at_time_ago_in_s) if alert.max_loaded_at_time_ago_in_s else 'N/A'}"
            )
            self.client.addSection(section)

        if alert.status != "runtime error":
            section = cardsection()
            section.activityTitle("*Last Record At*")
            section.activityText(f"{alert.max_loaded_at}")
            self.client.addSection(section)

        if alert.status != "runtime error":
            section = cardsection()
            section.activityTitle("*Sampled At*")
            section.activityText(f"{alert.snapshotted_at_str}")
            self.client.addSection(section)

        if alert.error_after:
            section = cardsection()
            section.activityTitle("*Error after*")
            section.activityText(f"`{alert.error_after}`")
            self.client.addSection(section)

        if alert.error_after:
            section = cardsection()
            section.activityTitle("*Warn after*")
            section.activityText(f"`{alert.warn_after}`")
            self.client.addSection(section)

        if alert.error_after:
            section = cardsection()
            section.activityTitle("*Filter*")
            section.activityText(f"`{alert.filter}`")
            self.client.addSection(section)

        if alert.path:
            section = cardsection()
            section.activityTitle("*Path*")
            section.activityText(f"`{alert.path}`")
            self.client.addSection(section)

    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, *args, **kwargs
    ):
        alerts = alert.alerts
        title = f"{self._get_display_name(alert.status)}: {alert.summary}"
        subtitle = ""

        if alert.model_errors:
            subtitle = (
                subtitle
                + (" | " + f"&#x1F635; Model errors: {len(alert.model_errors)}")
                if subtitle
                else f"&#x1F635; Model errors: {len(alert.model_errors)}"
            )
        if alert.test_failures:
            subtitle = (
                subtitle
                + (" | " + f"&#x1F53A; Test failures: {len(alert.test_failures)}")
                if subtitle
                else f"&#x1F53A; Test failures: {len(alert.test_failures)}"
            )
        if alert.test_warnings:
            subtitle = (
                subtitle
                + (" | " + f"&#x26A0; Test warnings: {len(alert.test_warnings)}")
                if subtitle
                else f"&#x26A0; Test warnings: {len(alert.test_warnings)}"
            )
        if alert.test_errors:
            subtitle = (
                subtitle + (" | " + f"&#x2757; Test errors: {len(alert.test_errors)}")
                if subtitle
                else f"&#x2757; Test errors: {len(alert.test_errors)}"
            )

        self._add_report_link_if_applicable(alert)

        self.client.title(title)
        self.client.text(subtitle)

        tags = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.tags or [] for alert in alerts]
        )
        owners = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.owners or [] for alert in alerts]
        )
        subscribers = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.subscribers or [] for alert in alerts]
        )

        section = cardsection()
        section.activityTitle("*Tags*")
        section.activityText(f'_{tags if tags else "No tags"}_')
        self.client.addSection(section)

        section = cardsection()
        section.activityTitle("*Owners*")
        section.activityText(f'_{owners if owners else "No owners"}_')
        self.client.addSection(section)

        section = cardsection()
        section.activityTitle("*Subscribers*")
        section.activityText(f'_{subscribers if subscribers else "No subscribers"}_')
        self.client.addSection(section)

        if alert.model_errors:
            section = cardsection()
            section.activityTitle("*Model errors*")
            section.activitySubtitle(
                f"{self._get_model_error_block_header(alert.model_errors)}"
            )
            section.activityText(
                f"{self._get_model_error_block_body(alert.model_errors)}"
            )
            self.client.addSection(section)

        if alert.test_failures:
            section = cardsection()
            section.activityTitle("*Test failures*")
            rows = [alert.concise_name for alert in alert.test_failures]
            text = "\n".join([f"&#x1F53A; {row}" for row in rows])
            section.activityText(text)
            self.client.addSection(section)

        if alert.test_warnings:
            section = cardsection()
            section.activityTitle("*Test warnings*")
            rows = [alert.concise_name for alert in alert.test_warnings]
            text = "\n".join([f"&#x26A0; {row}" for row in rows])
            section.activityText(text)
            self.client.addSection(section)

        if alert.test_errors:
            section = cardsection()
            section.activityTitle("*Test errors*")
            rows = [alert.concise_name for alert in alert.test_errors]
            text = "\n".join([f"&#x2757; {row}" for row in rows])
            section.activityText(text)
            self.client.addSection(section)

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
        # Since the title can never be truncated and the text can be truncated by Teams, I think it is good to have a title + text in the fallback template
        self.client.title("Oops, we failed to format the alert ! -_-'")
        self.client.text(
            f"Please share this with the Elementary team via <https://elementary-data.com/community> or a <https://github.com/elementary-data/elementary/issues/new|GitHub> issue. \n\n```{json.dumps(alert.data, indent=2)}"
        )

    def _get_test_message_template(self, *args, **kwargs):
        self.client.title("This is a test message generated by Elementary!")
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
            sent_successfully = fallback_sent_successfully
        # Resetting the client so that it does not cache the message of other alerts
        self.client = self._initial_client()

        return sent_successfully

    @staticmethod
    def _get_display_name(alert_status: Optional[str]) -> str:
        if alert_status is None:
            return "Unknown"
        return STATUS_DISPLAYS.get(alert_status, {}).get("display_name", alert_status)

    @staticmethod
    def _get_model_error_block_header(
        model_error_alerts: List[ModelAlertModel],
    ) -> List:
        if len(model_error_alerts) == 0:
            return []
        result = []
        for model_error_alert in model_error_alerts:
            if model_error_alert.message:
                result.extend(["*Result message*"])
        return result

    @staticmethod
    def _get_model_error_block_body(model_error_alerts: List[ModelAlertModel]) -> str:
        if len(model_error_alerts) == 0:
            return ""
        for model_error_alert in model_error_alerts:
            if model_error_alert.message:
                return f"```{model_error_alert.message.strip()}```"
        return ""

    def send_test_message(self, *args, **kwargs) -> bool:
        self._get_test_message_template()
        return self.client.send_message()
