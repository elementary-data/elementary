import json
import re
from typing import Optional, Union

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.schema import SlackMessageSchema
from elementary.monitor.alerts.alert import Alert
from elementary.monitor.alerts.report_link_utils import get_test_runs_link
from elementary.monitor.fetchers.alerts.normalized_alert import (
    COLUMN_FIELD,
    DESCRIPTION_FIELD,
    OWNERS_FIELD,
    RESULT_MESSAGE_FIELD,
    SUBSCRIBERS_FIELD,
    TABLE_FIELD,
    TAGS_FIELD,
    TEST_PARAMS_FIELD,
    TEST_QUERY_FIELD,
    TEST_RESULTS_SAMPLE_FIELD,
)
from elementary.utils.json_utils import try_load_json
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class TestAlert(Alert):
    TABLE_NAME = "alerts"
    __test__ = False  # Mark for pytest - The class name starts with "Test" which throws warnings on pytest runs

    def __init__(
        self,
        model_unique_id: str,
        test_unique_id: str,
        elementary_unique_id: str,
        test_name: str,
        test_created_at: Optional[str] = None,
        test_meta: Optional[Union[str, dict]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.test_meta = try_load_json(test_meta) or {}
        self.model_unique_id = model_unique_id
        self.test_unique_id = test_unique_id
        self.elementary_unique_id = elementary_unique_id
        self.test_created_at = test_created_at
        self.test_name = test_name
        self.test_display_name = self.display_name(test_name) if test_name else ""
        self.alerts_table = TestAlert.TABLE_NAME

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
        self.test_short_name = test_short_name
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
        self.test_description = self._get_test_description()
        self.error_message = self.test_results_description
        self.column_name = column_name or ""
        self.severity = severity

        self.failed_rows_count = -1
        if self.status != "pass" and self.error_message:
            found_rows_number_match = re.search(r"\d+", self.error_message)
            if found_rows_number_match:
                found_rows_number = found_rows_number_match.group()
                self.failed_rows_count = int(found_rows_number)

    def _get_test_description(self):
        if self.test_meta:
            return self.test_meta.get("description")
        elif self.meta:
            return self.meta.get("description")
        else:
            return None

    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        icon = self.slack_message_builder.get_slack_status_icon(self.status)

        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))

        title = [
            self.slack_message_builder.create_header_block(f"{icon} dbt test alert")
        ]
        if self.alert_suppression_interval:
            title.extend(
                [
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Test:* {self.test_short_name or self.test_name} - {self.test_sub_type_display_name}     |",
                            f"*Status:* {self.status}",
                        ],
                    ),
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Time*: {self.detected_at_str}     |",
                            f"*Suppression interval:* {self.alert_suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.slack_message_builder.create_context_block(
                    [
                        f"*Test:* {self.test_short_name or self.test_name} - {self.test_sub_type_display_name}     |",
                        f"*Status:* {self.status}     |",
                        f"*{self.detected_at_str}*",
                    ],
                ),
            )

        test_runs_report_link = get_test_runs_link(
            self.report_url, self.elementary_unique_id
        )
        if test_runs_report_link:
            report_link = self.slack_message_builder.create_context_block(
                [
                    f"<{test_runs_report_link.url}|{test_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        if self.alert_fields is None:
            self.alert_fields = []

        preview = []
        if TABLE_FIELD in self.alert_fields:
            preview.append(
                self.slack_message_builder.create_text_section_block(
                    f"*Table*\n{self.table_full_name}"
                )
            )

        compacted_sections = []
        if COLUMN_FIELD in self.alert_fields:
            compacted_sections.append(f"*Column*\n{self.column_name or '_No column_'}")
        if TAGS_FIELD in self.alert_fields:
            tags = self.slack_message_builder.prettify_and_dedup_list(self.tags or [])
            compacted_sections.append(f"*Tags*\n{tags or '_No tags_'}")
        if OWNERS_FIELD in self.alert_fields:
            owners = self.slack_message_builder.prettify_and_dedup_list(
                self.owners or []
            )
            compacted_sections.append(f"*Owners*\n{owners or '_No owners_'}")
        if SUBSCRIBERS_FIELD in self.alert_fields:
            subscribers = self.slack_message_builder.prettify_and_dedup_list(
                self.subscribers or []
            )
            compacted_sections.append(
                f'*Subscribers*\n{subscribers or "_No subscribers_"}'
            )
        if compacted_sections:
            preview.extend(
                self.slack_message_builder.create_compacted_sections_blocks(
                    compacted_sections
                )
            )

        if DESCRIPTION_FIELD in self.alert_fields:
            if self.test_description:
                preview.extend(
                    [
                        self.slack_message_builder.create_text_section_block(
                            "*Description*"
                        ),
                        self.slack_message_builder.create_context_block(
                            [self.test_description]
                        ),
                    ]
                )
            else:
                preview.append(
                    self.slack_message_builder.create_text_section_block(
                        "*Description*\n_No description_"
                    )
                )

        result = []
        if RESULT_MESSAGE_FIELD in self.alert_fields and self.error_message:
            result.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Result message*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"```{self.error_message.strip()}```"
                    ),
                ]
            )

        if TEST_RESULTS_SAMPLE_FIELD in self.alert_fields and self.test_rows_sample:
            result.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Test results sample*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"```{self.test_rows_sample}```"
                    ),
                ]
            )

        if TEST_QUERY_FIELD in self.alert_fields and self.test_results_query:
            result.append(
                self.slack_message_builder.create_context_block(["*Test query*"])
            )

            msg = f"```{self.test_results_query}```"
            if len(msg) > SectionBlock.text_max_length:
                msg = (
                    f"_The test query was too long, here's a query to get it._\n"
                    f"```SELECT test_results_query FROM {self.elementary_database_and_schema}.elementary_test_results WHERE test_execution_id = '{self.id}'```"
                )
            result.append(self.slack_message_builder.create_text_section_block(msg))

        configuration = []
        if TEST_PARAMS_FIELD in self.alert_fields and self.test_params:
            configuration.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Test parameters*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"```{self.test_params}```"
                    ),
                ]
            )

        return self.slack_message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    @property
    def concise_name(self):
        return f"{self.test_short_name or self.test_name}"


class ElementaryTestAlert(DbtTestAlert):
    def to_slack(self, is_slack_workflow: bool = False) -> SlackMessageSchema:
        icon = self.slack_message_builder.get_slack_status_icon(self.status)

        anomalous_value = None
        if self.test_type == "schema_change":
            alert_title = "Schema change detected"
        elif self.test_type == "anomaly_detection":
            alert_title = "Data anomaly detected"
            anomalous_value = self.other or None
        else:
            raise ValueError("Invalid test type.", self.test_type)

        if is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(self.__dict__))

        title = [
            self.slack_message_builder.create_header_block(f"{icon} {alert_title}"),
        ]
        if self.alert_suppression_interval:
            title.extend(
                [
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Test:* {self.test_short_name or self.test_name} - {self.test_sub_type_display_name}     |",
                            f"*Status:* {self.status}",
                        ],
                    ),
                    self.slack_message_builder.create_context_block(
                        [
                            f"*Time*: {self.detected_at_str}     |",
                            f"*Suppression interval:* {self.alert_suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.slack_message_builder.create_context_block(
                    [
                        f"*Test:* {self.test_short_name or self.test_name} - {self.test_sub_type_display_name}     |",
                        f"*Status:* {self.status}     |",
                        f"*{self.detected_at_str}*",
                    ],
                ),
            )

        test_runs_report_link = get_test_runs_link(
            self.report_url, self.elementary_unique_id
        )
        if test_runs_report_link:
            report_link = self.slack_message_builder.create_context_block(
                [
                    f"<{test_runs_report_link.url}|{test_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        if self.alert_fields is None:
            self.alert_fields = []

        preview = []
        if TABLE_FIELD in self.alert_fields:
            preview.append(
                self.slack_message_builder.create_text_section_block(
                    f"*Table*\n{self.table_full_name}"
                )
            )

        compacted_sections = []
        if COLUMN_FIELD in self.alert_fields:
            compacted_sections.append(f"*Column*\n{self.column_name or '_No column_'}")
        if TAGS_FIELD in self.alert_fields:
            tags = self.slack_message_builder.prettify_and_dedup_list(self.tags or [])
            compacted_sections.append(f"*Tags*\n{tags or '_No tags_'}")
        if OWNERS_FIELD in self.alert_fields:
            owners = self.slack_message_builder.prettify_and_dedup_list(
                self.owners or []
            )
            compacted_sections.append(f"*Owners*\n{owners or '_No owners_'}")
        if SUBSCRIBERS_FIELD in self.alert_fields:
            subscribers = self.slack_message_builder.prettify_and_dedup_list(
                self.subscribers or []
            )
            compacted_sections.append(
                f'*Subscribers*\n{subscribers or "_No subscribers_"}'
            )
        if compacted_sections:
            preview.extend(
                self.slack_message_builder.create_compacted_sections_blocks(
                    compacted_sections
                )
            )

        if DESCRIPTION_FIELD in self.alert_fields:
            if self.test_description:
                preview.extend(
                    [
                        self.slack_message_builder.create_text_section_block(
                            "*Description*"
                        ),
                        self.slack_message_builder.create_context_block(
                            [self.test_description]
                        ),
                    ]
                )
            else:
                preview.append(
                    self.slack_message_builder.create_text_section_block(
                        "*Description*\n_No description_"
                    )
                )

        result = []
        if RESULT_MESSAGE_FIELD in self.alert_fields and self.error_message:
            result.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Result message*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"```{self.error_message.strip()}```"
                    ),
                ]
            )

        if TEST_RESULTS_SAMPLE_FIELD in self.alert_fields and anomalous_value:
            result.append(
                self.slack_message_builder.create_context_block(
                    ["*Test results sample*"]
                )
            )
            messages = []
            if self.column_name:
                messages.append(f"*Column*: {self.column_name}     |")
            messages.append(f"*Anomalous Values*: {anomalous_value}")
            result.append(self.slack_message_builder.create_context_block(messages))

        configuration = []
        if TEST_PARAMS_FIELD in self.alert_fields and self.test_params:
            configuration.extend(
                [
                    self.slack_message_builder.create_context_block(
                        ["*Test parameters*"]
                    ),
                    self.slack_message_builder.create_text_section_block(
                        f"```{self.test_params}```"
                    ),
                ]
            )

        return self.slack_message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    @property
    def concise_name(self):
        return f"{self.test_short_name or self.test_name} - {self.test_sub_type_display_name}"
