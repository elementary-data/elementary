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


class TestAlert(Alert):
    TABLE_NAME = "alerts"

    def __init__(
        self,
        model_unique_id: str,
        test_unique_id: str,
        test_created_at: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.model_unique_id = model_unique_id
        self.test_unique_id = test_unique_id
        self.test_created_at = test_created_at

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
        icon = ":small_red_triangle:"
        if self.status == "warn":
            icon = ":warning:"
        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))
        slack_message = {"attachments": [{"blocks": []}]}
        self._add_text_section_to_slack_msg(slack_message, f"{icon} *dbt test alert*")
        self._add_fields_section_to_slack_msg(
            slack_message,
            [
                f"*Table*\n{self.table_full_name}",
                f"*When*\n{self.detected_at.strftime(DATETIME_FORMAT)}",
            ],
            divider=True,
        )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [f"*Status*\n{self.status}", f"*Test name*\n{self.test_name}"],
        )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"]
        )
        if self.subscribers:
            self._add_fields_section_to_slack_msg(
                slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}']
            )
        if self.error_message:
            self._add_text_section_to_slack_msg(
                slack_message,
                f"*Error Message*\n```{self.error_message}```",
                divider=True,
            )
        if self.column_name:
            self._add_text_section_to_slack_msg(
                slack_message, f"*Column*\n{self.column_name}", divider=True
            )
        if self.test_params:
            self._add_text_section_to_slack_msg(
                slack_message, f"*Test Parameters*\n`{self.test_params}`", divider=True
            )
        if self.test_results_query:
            msg = f"*Test Query*\n```{self.test_results_query}```"
            if len(msg) > SectionBlock.text_max_length:
                msg = (
                    f"*Query for Test's Query*\n"
                    f"_The test query was too long, here's a query to get it._\n"
                    f"```SELECT test_results_query FROM {self.elementary_database_and_schema}.elementary_test_results WHERE test_execution_id = '{self.id}'```"
                )
            self._add_text_section_to_slack_msg(slack_message, msg, divider=True)

        if self.test_rows_sample:
            self._add_text_section_to_slack_msg(
                slack_message,
                f"*Test Results Sample*\n`{self.test_rows_sample}`",
                divider=True,
            )
        return SlackMessageSchema(attachments=slack_message["attachments"])

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
        slack_message = {"attachments": [{"blocks": []}]}
        icon = ":small_red_triangle:"
        if self.status == "warn":
            icon = ":warning:"
        self._add_text_section_to_slack_msg(slack_message, f"{icon} *{alert_title}*")
        self._add_fields_section_to_slack_msg(
            slack_message,
            [
                f"*Table*\n{self.table_full_name}",
                f"*When*\n{self.detected_at.strftime(DATETIME_FORMAT)}",
            ],
            divider=True,
        )
        self._add_fields_section_to_slack_msg(
            slack_message,
            [
                f"*Test name*\n{self.test_name}",
                f"*{sub_type_title}:*\n{self.test_sub_type_display_name}",
            ],
        )
        self._add_fields_section_to_slack_msg(
            slack_message, [f"*Owners*\n{self.owners}", f"*Tags*\n{self.tags}"]
        )
        if self.subscribers:
            self._add_fields_section_to_slack_msg(
                slack_message, [f'*Subscribers*\n{", ".join(set(self.subscribers))}']
            )
        if self.error_message:
            self._add_text_section_to_slack_msg(
                slack_message,
                f"*Error Message*\n```{self.error_message}```",
                divider=True,
            )
        column_msgs = []
        if self.column_name:
            column_msgs.append(f"*Column*\n{self.column_name}")
        if anomalous_value:
            column_msgs.append(f"*Anomalous Values*\n{anomalous_value}")
        if column_msgs:
            self._add_fields_section_to_slack_msg(
                slack_message, column_msgs, divider=True
            )
        if self.test_params:
            self._add_text_section_to_slack_msg(
                slack_message, f"*Test Parameters*\n`{self.test_params}`", divider=True
            )
        return SlackMessageSchema(attachments=slack_message["attachments"])

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
            },
            "test_results": test_alerts,
            "test_runs": test_runs,
        }
