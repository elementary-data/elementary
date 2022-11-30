import json
import re
from typing import Optional

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger
from elementary.utils.time import DATETIME_FORMAT

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
RESULT_SECTION_FIELDS = [
    RESULT_MESSAGE_FIELD,
    TEST_QUERY_FIELD,
    TEST_RESULTS_SAMPLE_FIELD,
]
CONFIGURATION_SECTION_FIELDS = [TEST_PARAMS_FIELD]


class TestAlert(Alert):
    TABLE_NAME = "alerts"

    def __init__(
        self,
        model_unique_id: str,
        test_unique_id: str,
        test_created_at: Optional[str] = None,
        test_meta: Optional[str] = None,
        model_meta: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.model_unique_id = model_unique_id
        self.test_unique_id = test_unique_id
        self.test_created_at = test_created_at
        self.test_meta = try_load_json(test_meta) or {}
        self.model_meta = try_load_json(model_meta) or {}

    def to_test_alert_api_dict(self) -> dict:
        raise NotImplementedError

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        raise NotImplementedError

    @staticmethod
    def display_name(str_value: str) -> str:
        return str_value.replace("_", " ").title()

    @staticmethod
    def create_test_alert_from_dict(**test_alert_dict) -> Optional["TestAlert"]:
        if test_alert_dict.get("test_type") == "dbt_test":
            return DbtTestAlert(**test_alert_dict)
        return ElementaryTestAlert(**test_alert_dict)

    def get_alert_fields(self) -> Optional[list]:
        # If there is no alerts_fields in the test meta object,
        # we return the model alerts_fields from the model meta object.
        # The fallback is DEFAULT_ALERT_FIELDS.
        return (
            self.test_meta.get("alert_fields")
            if self.test_meta.get("alert_fields")
            else self.model_meta.get("alert_fields", DEFAULT_ALERT_FIELDS)
        )


class DbtTestAlert(TestAlert):
    def __init__(
        self,
        table_name,
        column_name,
        test_type,
        test_sub_type,
        test_results_description,
        test_results_query,
        test_rows_sample,
        other,
        test_name,
        test_params,
        severity,
        test_runs=None,
        test_short_name=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.test_type = test_type
        self.table_name = table_name
        table_full_name_parts = [
            name for name in [self.database_name, self.schema_name, table_name] if name
        ]
        self.table_full_name = ".".join(table_full_name_parts).lower()
        self.test_name = test_name
        self.test_short_name = test_short_name
        self.test_display_name = self.display_name(test_name) if test_name else ""
        self.other = other
        self.test_sub_type = test_sub_type or ""
        self.test_sub_type_display_name = (
            self.display_name(test_sub_type) if test_sub_type else ""
        )
        self.test_results_query = (
            test_results_query.strip() if test_results_query else ""
        )
        self.test_rows_sample = test_rows_sample or ""
        self.test_runs = test_runs or ""
        self.test_params = test_params
        self.test_results_description = (
            test_results_description.capitalize() if test_results_description else ""
        )
        self.test_description = (
            self.test_meta.get("description") if self.test_meta else ""
        )
        self.error_message = self.test_results_description
        self.column_name = column_name or ""
        self.severity = severity

        self.failed_rows_count = -1
        if self.status != "pass" and self.error_message:
            found_rows_number = re.search(r"\d+", self.error_message)
            if found_rows_number:
                found_rows_number = found_rows_number.group()
                self.failed_rows_count = int(found_rows_number)

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        # Notice!
        # Blocks will always be fully displayed
        # Only the first 5 attachments will be displayed before Slack collapse the rest into a "show more" section
        alert_fields = self.get_alert_fields()
        icon = self._get_slack_status_icon()

        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))
        slack_message = self._initial_slack_message()

        # Alert info section
        self._add_header_to_slack_msg(slack_message, f"{icon} dbt test alert")
        self._add_context_to_slack_msg(
            slack_message,
            [
                f"*Test:* {self.test_short_name if self.test_short_name else self.test_name}     |",
                f"*Status:* {self.status}     |",
                f"*{self.detected_at.strftime(DATETIME_FORMAT)}*",
            ],
        )
        self._add_divider(slack_message)

        if TABLE_FIELD in alert_fields:
            self._add_text_section_to_slack_msg(
                slack_message,
                f"*Table*\n{self.table_full_name}",
                add_to_attachment=True,
            )

        compacted_sections = []

        if COLUMN_FIELD in alert_fields:
            compacted_sections.append(
                f"*Column*\n{self.column_name if self.column_name else '_No column_'}"
            )

        if TAGS_FIELD in alert_fields:
            compacted_sections.append(
                f"*Tags*\n{self.tags if self.tags else '_No tags_'}"
            )

        if OWNERS_FIELD in alert_fields:
            compacted_sections.append(
                f"*Owners*\n{self.owners if self.owners else '_No owners_'}"
            )

        if SUBSCRIBERS_FIELD in alert_fields:
            compacted_sections.append(
                f'*Subscribers*\n{", ".join(set(self.subscribers)) if self.subscribers else "_No subscribers_"}'
            )

        self._add_compacted_sections_to_slack_msg(
            slack_message,
            compacted_sections,
            add_to_attachment=True,
        )

        if DESCRIPTION_FIELD in alert_fields:
            if self.test_description:
                self._add_text_section_to_slack_msg(
                    slack_message, "*Description*", add_to_attachment=True
                )

                self._add_context_to_slack_msg(
                    slack_message,
                    [self.test_description],
                    add_to_attachment=True,
                )
            else:
                self._add_text_section_to_slack_msg(
                    slack_message,
                    "*Description*\n_No description_",
                    add_to_attachment=True,
                )
                self._add_empty_section_to_slack_msg(
                    slack_message, add_to_attachment=True
                )

        # Result sectiom
        if any(
            result_field == alert_field
            for result_field in RESULT_SECTION_FIELDS
            for alert_field in alert_fields
        ):
            self._add_text_section_to_slack_msg(
                slack_message, f":mag: *Result*", add_to_attachment=True
            )
            self._add_divider(slack_message, add_to_attachment=True)

            if RESULT_MESSAGE_FIELD in alert_fields and self.error_message:
                self._add_context_to_slack_msg(
                    slack_message, ["*Result message*"], add_to_attachment=True
                )

                self._add_text_section_to_slack_msg(
                    slack_message,
                    f"```{self.error_message.strip()}```",
                    add_to_attachment=True,
                )

            if TEST_RESULTS_SAMPLE_FIELD in alert_fields and self.test_rows_sample:
                self._add_context_to_slack_msg(
                    slack_message, ["*Test results sample*"], add_to_attachment=True
                )

                self._add_text_section_to_slack_msg(
                    slack_message,
                    f"`{self.test_rows_sample}`",
                    add_to_attachment=True,
                )

            if TEST_QUERY_FIELD in alert_fields and self.test_results_query:
                self._add_context_to_slack_msg(
                    slack_message, ["*Test query*"], add_to_attachment=True
                )

                msg = f"```{self.test_results_query}```"
                if len(msg) > SectionBlock.text_max_length:
                    msg = (
                        f"_The test query was too long, here's a query to get it._\n"
                        f"```SELECT test_results_query FROM {self.elementary_database_and_schema}.elementary_test_results WHERE test_execution_id = '{self.id}'```"
                    )
                self._add_text_section_to_slack_msg(
                    slack_message, msg, add_to_attachment=True
                )

        # Configuration sectiom
        if any(
            configuration_field == alert_field
            for configuration_field in CONFIGURATION_SECTION_FIELDS
            for alert_field in alert_fields
        ):
            self._add_text_section_to_slack_msg(
                slack_message,
                f":hammer_and_wrench: *Configuration*",
                add_to_attachment=True,
            )
            self._add_divider(slack_message, add_to_attachment=True)

            if TEST_PARAMS_FIELD in alert_fields and self.test_params:
                self._add_context_to_slack_msg(
                    slack_message, ["*Test parameters*"], add_to_attachment=True
                )

                self._add_text_section_to_slack_msg(
                    slack_message,
                    f"`{self.test_params}`",
                    add_to_attachment=True,
                )

        # Create Slack message
        return SlackMessageSchema(**slack_message)

    def to_test_alert_api_dict(self):
        test_runs = (
            {**self.test_runs, "display_name": self.test_display_name}
            if self.test_runs
            else {}
        )

        return {
            "metadata": {
                "test_unique_id": self.test_unique_id,
                "database_name": self.database_name,
                "schema_name": self.schema_name,
                "table_name": self.table_name,
                "column_name": self.column_name,
                "test_name": self.test_name,
                "test_display_name": self.test_display_name,
                "latest_run_time": self.detected_at.isoformat(),
                "latest_run_time_utc": self.detected_at_utc.isoformat(),
                "latest_run_status": self.status,
                "model_unique_id": self.model_unique_id,
                "table_unique_id": self.table_full_name,
                "test_type": self.test_type,
                "test_sub_type": self.test_sub_type,
                "test_query": self.test_results_query,
                "test_params": self.test_params,
                "test_created_at": self.test_created_at,
                "description": self.test_description,
            },
            "test_results": {
                "display_name": self.test_display_name + " - failed results sample",
                "results_sample": self.test_rows_sample,
                "error_message": self.error_message,
                "failed_rows_count": self.failed_rows_count,
            },
            "test_runs": test_runs,
        }


class ElementaryTestAlert(DbtTestAlert):
    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        # Notice!
        # Blocks will always be fully displayed
        # Only the first 5 attachments will be displayed before Slack collapse the rest into a "show more" section
        alert_fields = self.get_alert_fields()
        icon = self._get_slack_status_icon()

        anomalous_value = None
        if self.test_type == "schema_change":
            alert_title = "Schema change detected"
            sub_type_title = "Change Type"
        elif self.test_type == "anomaly_detection":
            alert_title = "Data anomaly detected"
            sub_type_title = "Anomaly Type"
            anomalous_value = self.other if self.other else None
        else:
            raise ValueError("Invalid test type.", self.test_type)

        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))

        slack_message = self._initial_slack_message()

        # Alert info section
        self._add_header_to_slack_msg(slack_message, f"{icon} {alert_title}")
        self._add_context_to_slack_msg(
            slack_message,
            [
                f"*Test:* {self.test_short_name if self.test_short_name else self.test_name} - {self.test_sub_type_display_name}     |",
                f"*Status:* {self.status}     |",
                f"*{self.detected_at.strftime(DATETIME_FORMAT)}*",
            ],
        )
        self._add_divider(slack_message)

        if TABLE_FIELD in alert_fields:
            self._add_text_section_to_slack_msg(
                slack_message,
                f"*Table*\n{self.table_full_name}",
                add_to_attachment=True,
            )

        compacted_sections = []

        if COLUMN_FIELD in alert_fields:
            compacted_sections.append(
                f"*Column*\n{self.column_name if self.column_name else '_No column_'}"
            )

        if TAGS_FIELD in alert_fields:
            compacted_sections.append(
                f"*Tags*\n{self.tags if self.tags else '_No tags_'}"
            )

        if OWNERS_FIELD in alert_fields:
            compacted_sections.append(
                f"*Owners*\n{self.owners if self.owners else '_No owners_'}"
            )

        if SUBSCRIBERS_FIELD in alert_fields:
            compacted_sections.append(
                f'*Subscribers*\n{", ".join(set(self.subscribers)) if self.subscribers else "_No subscribers_"}'
            )

        self._add_compacted_sections_to_slack_msg(
            slack_message,
            compacted_sections,
            add_to_attachment=True,
        )

        if DESCRIPTION_FIELD in alert_fields:
            if self.test_description:
                self._add_text_section_to_slack_msg(
                    slack_message, "*Description*", add_to_attachment=True
                )

                self._add_context_to_slack_msg(
                    slack_message,
                    [self.test_description],
                    add_to_attachment=True,
                )
            else:
                self._add_text_section_to_slack_msg(
                    slack_message,
                    "*Description*\n_No description_",
                    add_to_attachment=True,
                )
                self._add_empty_section_to_slack_msg(
                    slack_message, add_to_attachment=True
                )

        # Result sectiom
        if any(
            result_field == alert_field
            for result_field in RESULT_SECTION_FIELDS
            for alert_field in alert_fields
        ):
            self._add_text_section_to_slack_msg(
                slack_message, f":mag: *Result*", add_to_attachment=True
            )
            self._add_divider(slack_message, add_to_attachment=True)

            if RESULT_MESSAGE_FIELD in alert_fields and self.error_message:
                self._add_context_to_slack_msg(
                    slack_message, ["*Result message*"], add_to_attachment=True
                )

                self._add_text_section_to_slack_msg(
                    slack_message,
                    f"```{self.error_message.strip()}```",
                    add_to_attachment=True,
                )

            if TEST_RESULTS_SAMPLE_FIELD in alert_fields and anomalous_value:
                self._add_context_to_slack_msg(
                    slack_message, ["*Test results sample*"], add_to_attachment=True
                )

                messagess = []
                if self.column_name:
                    messagess.append(f"*Column*: {self.column_name}     |")
                messagess.append(f"*Anomalous Values*: {anomalous_value}")
                if messagess:
                    self._add_context_to_slack_msg(
                        slack_message, messagess, add_to_attachment=True
                    )

        # Configuration sectiom
        if any(
            configuration_field == alert_field
            for configuration_field in CONFIGURATION_SECTION_FIELDS
            for alert_field in alert_fields
        ):
            self._add_text_section_to_slack_msg(
                slack_message,
                f":hammer_and_wrench: *Configuration*",
                add_to_attachment=True,
            )
            self._add_divider(slack_message, add_to_attachment=True)

            if TEST_PARAMS_FIELD in alert_fields and self.test_params:
                self._add_context_to_slack_msg(
                    slack_message, ["*Test parameters*"], add_to_attachment=True
                )

                self._add_text_section_to_slack_msg(
                    slack_message,
                    f"`{self.test_params}`",
                    add_to_attachment=True,
                )

        # Create Slack message
        return SlackMessageSchema(**slack_message)

    def to_test_alert_api_dict(self):
        test_params = try_load_json(self.test_params) or {}
        test_alerts = None
        if self.test_type == "anomaly_detection":
            timestamp_column = test_params.get("timestamp_column")
            sensitivity = test_params.get("sensitivity")
            test_params = {
                "timestamp_column": timestamp_column,
                "anomaly_threshold": sensitivity,
            }
            if self.test_rows_sample and self.test_sub_type != "dimension":
                self.test_rows_sample.sort(key=lambda metric: metric.get("end_time"))
            test_alerts = {
                "display_name": self.test_sub_type_display_name,
                "metrics": self.test_rows_sample,
                "result_description": self.test_results_description,
            }
        elif self.test_type == "schema_change":
            test_alerts = {
                "display_name": self.test_sub_type_display_name.lower(),
                "result_description": self.test_results_description,
            }
        test_runs = (
            {**self.test_runs, "display_name": self.test_sub_type_display_name}
            if self.test_runs
            else {}
        )

        return {
            "metadata": {
                "test_unique_id": self.test_unique_id,
                "database_name": self.database_name,
                "schema_name": self.schema_name,
                "table_name": self.table_name,
                "column_name": self.column_name,
                "test_name": self.test_name,
                "test_display_name": self.test_display_name,
                "latest_run_time": self.detected_at.isoformat(),
                "latest_run_time_utc": self.detected_at_utc.isoformat(),
                "latest_run_status": self.status,
                "model_unique_id": self.model_unique_id,
                "table_unique_id": self.table_full_name,
                "test_type": self.test_type,
                "test_sub_type": self.test_sub_type,
                "test_query": self.test_results_query,
                "test_params": test_params,
                "test_created_at": self.test_created_at,
                "description": self.test_description,
            },
            "test_results": test_alerts,
            "test_runs": test_runs,
        }
