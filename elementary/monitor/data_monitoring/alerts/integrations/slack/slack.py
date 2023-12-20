import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.client import SlackClient, SlackWebClient
from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.slack.message_builder import (
    SlackAlertMessageBuilder,
)
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    get_model_runs_link,
    get_model_test_runs_link,
    get_test_runs_link,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.json_utils import (
    list_of_lists_of_strings_to_comma_delimited_unique_strings,
)

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


class SlackIntegration(BaseIntegration):
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
        self.message_builder = SlackAlertMessageBuilder()
        super().__init__()

        # Enforce typing
        self.client: SlackClient

    def _initial_client(self, *args, **kwargs) -> SlackClient:
        slack_client = SlackClient.create_client(
            config=self.config, tracking=self.tracking
        )
        if not slack_client:
            raise Exception("Could not initial Slack client")
        return slack_client

    def _get_alert_template(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs,
    ) -> SlackMessageSchema:
        if self.config.is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(alert.data, sort_keys=True))
        return super()._get_alert_template(alert, *args, **kwargs)

    def _get_dbt_test_template(
        self, alert: TestAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        icon = self.message_builder.get_slack_status_icon(alert.status)

        title = [self.message_builder.create_header_block(f"{icon} dbt test alert")]
        if alert.suppression_interval:
            title.extend(
                [
                    self.message_builder.create_context_block(
                        [
                            f"*Test:* {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                            f"*Status:* {alert.status}",
                        ],
                    ),
                    self.message_builder.create_context_block(
                        [
                            f"*Time*: {alert.detected_at_str}     |",
                            f"*Suppression interval:* {alert.suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.message_builder.create_context_block(
                    [
                        f"*Test:* {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                        f"*Status:* {alert.status}     |",
                        f"*{alert.detected_at_str}*",
                    ],
                ),
            )

        test_runs_report_link = get_test_runs_link(
            alert.report_url, alert.elementary_unique_id
        )
        if test_runs_report_link:
            report_link = self.message_builder.create_context_block(
                [
                    f"<{test_runs_report_link.url}|{test_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        preview = []
        if TABLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            preview.append(
                self.message_builder.create_text_section_block(
                    f"*Table*\n{alert.table_full_name}"
                )
            )

        compacted_sections = []
        if COLUMN_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            compacted_sections.append(f"*Column*\n{alert.column_name or '_No column_'}")
        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = self.message_builder.prettify_and_dedup_list(alert.tags or [])
            compacted_sections.append(f"*Tags*\n{tags or '_No tags_'}")
        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = self.message_builder.prettify_and_dedup_list(alert.owners or [])
            compacted_sections.append(f"*Owners*\n{owners or '_No owners_'}")
        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = self.message_builder.prettify_and_dedup_list(
                alert.subscribers or []
            )
            compacted_sections.append(
                f'*Subscribers*\n{subscribers or "_No subscribers_"}'
            )
        if compacted_sections:
            preview.extend(
                self.message_builder.create_compacted_sections_blocks(
                    compacted_sections
                )
            )

        if DESCRIPTION_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            if alert.test_description:
                preview.extend(
                    [
                        self.message_builder.create_text_section_block("*Description*"),
                        self.message_builder.create_context_block(
                            [alert.test_description]
                        ),
                    ]
                )
            else:
                preview.append(
                    self.message_builder.create_text_section_block(
                        "*Description*\n_No description_"
                    )
                )

        result = []
        if (
            RESULT_MESSAGE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.error_message
        ):
            result.extend(
                [
                    self.message_builder.create_context_block(["*Result message*"]),
                    self.message_builder.create_text_section_block(
                        f"```{alert.error_message.strip()}```"
                    ),
                ]
            )

        if (
            TEST_RESULTS_SAMPLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_rows_sample
        ):
            result.extend(
                [
                    self.message_builder.create_context_block(
                        ["*Test results sample*"]
                    ),
                    self.message_builder.create_text_section_block(
                        f"```{alert.test_rows_sample}```"
                    ),
                ]
            )

        if (
            TEST_QUERY_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_results_query
        ):
            result.append(self.message_builder.create_context_block(["*Test query*"]))

            msg = f"```{alert.test_results_query}```"
            if len(msg) > SectionBlock.text_max_length:
                msg = (
                    f"_The test query was too long, here's a query to get it._\n"
                    f"```SELECT test_results_query FROM {alert.elementary_database_and_schema}.elementary_test_results WHERE test_execution_id = '{alert.id}'```"
                )
            result.append(self.message_builder.create_text_section_block(msg))

        configuration = []
        if (
            TEST_PARAMS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_params
        ):
            configuration.extend(
                [
                    self.message_builder.create_context_block(["*Test parameters*"]),
                    self.message_builder.create_text_section_block(
                        f"```{alert.test_params}```"
                    ),
                ]
            )

        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_elementary_test_template(
        self, alert: TestAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        icon = self.message_builder.get_slack_status_icon(alert.status)

        anomalous_value = None
        if alert.test_type == "schema_change":
            alert_title = "Schema change detected"
        elif alert.test_type == "anomaly_detection":
            alert_title = "Data anomaly detected"
            anomalous_value = alert.other or None
        else:
            raise ValueError("Invalid test type.", alert.test_type)

        title = [
            self.message_builder.create_header_block(f"{icon} {alert_title}"),
        ]
        if alert.suppression_interval:
            title.extend(
                [
                    self.message_builder.create_context_block(
                        [
                            f"*Test:* {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                            f"*Status:* {alert.status}",
                        ],
                    ),
                    self.message_builder.create_context_block(
                        [
                            f"*Time*: {alert.detected_at_str}     |",
                            f"*Suppression interval:* {alert.suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.message_builder.create_context_block(
                    [
                        f"*Test:* {alert.test_short_name or alert.test_name} - {alert.test_sub_type_display_name}     |",
                        f"*Status:* {alert.status}     |",
                        f"*{alert.detected_at_str}*",
                    ],
                ),
            )

        test_runs_report_link = get_test_runs_link(
            alert.report_url, alert.elementary_unique_id
        )
        if test_runs_report_link:
            report_link = self.message_builder.create_context_block(
                [
                    f"<{test_runs_report_link.url}|{test_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        preview = []
        if TABLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            preview.append(
                self.message_builder.create_text_section_block(
                    f"*Table*\n{alert.table_full_name}"
                )
            )

        compacted_sections = []
        if COLUMN_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            compacted_sections.append(f"*Column*\n{alert.column_name or '_No column_'}")
        if TAGS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            tags = self.message_builder.prettify_and_dedup_list(alert.tags or [])
            compacted_sections.append(f"*Tags*\n{tags or '_No tags_'}")
        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = self.message_builder.prettify_and_dedup_list(alert.owners or [])
            compacted_sections.append(f"*Owners*\n{owners or '_No owners_'}")
        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = self.message_builder.prettify_and_dedup_list(
                alert.subscribers or []
            )
            compacted_sections.append(
                f'*Subscribers*\n{subscribers or "_No subscribers_"}'
            )
        if compacted_sections:
            preview.extend(
                self.message_builder.create_compacted_sections_blocks(
                    compacted_sections
                )
            )

        if DESCRIPTION_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            if alert.test_description:
                preview.extend(
                    [
                        self.message_builder.create_text_section_block("*Description*"),
                        self.message_builder.create_context_block(
                            [alert.test_description]
                        ),
                    ]
                )
            else:
                preview.append(
                    self.message_builder.create_text_section_block(
                        "*Description*\n_No description_"
                    )
                )

        result = []
        if (
            RESULT_MESSAGE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.error_message
        ):
            result.extend(
                [
                    self.message_builder.create_context_block(["*Result message*"]),
                    self.message_builder.create_text_section_block(
                        f"```{alert.error_message.strip()}```"
                    ),
                ]
            )

        if (
            TEST_RESULTS_SAMPLE_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and anomalous_value
        ):
            result.append(
                self.message_builder.create_context_block(["*Test results sample*"])
            )
            messages = []
            if alert.column_name:
                messages.append(f"*Column*: {alert.column_name}     |")
            messages.append(f"*Anomalous Values*: {anomalous_value}")
            result.append(self.message_builder.create_context_block(messages))

        configuration = []
        if (
            TEST_PARAMS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS)
            and alert.test_params
        ):
            configuration.extend(
                [
                    self.message_builder.create_context_block(["*Test parameters*"]),
                    self.message_builder.create_text_section_block(
                        f"```{alert.test_params}```"
                    ),
                ]
            )

        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_model_template(
        self, alert: ModelAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        tags = self.message_builder.prettify_and_dedup_list(alert.tags)
        owners = self.message_builder.prettify_and_dedup_list(alert.owners)
        subscribers = self.message_builder.prettify_and_dedup_list(alert.subscribers)
        icon = self.message_builder.get_slack_status_icon(alert.status)

        title = [self.message_builder.create_header_block(f"{icon} dbt model alert")]
        if alert.suppression_interval:
            title.extend(
                [
                    self.message_builder.create_context_block(
                        [
                            f"*Model:* {alert.alias}     |",
                            f"*Status:* {alert.status}",
                        ],
                    ),
                    self.message_builder.create_context_block(
                        [
                            f"*Time:* {alert.detected_at_str}     |",
                            f"*Suppression interval:* {alert.suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.message_builder.create_context_block(
                    [
                        f"*Model:* {alert.alias}     |",
                        f"*Status:* {alert.status}     |",
                        f"*{alert.detected_at_str}*",
                    ],
                ),
            )

        model_runs_report_link = get_model_runs_link(
            alert.report_url, alert.model_unique_id
        )
        if model_runs_report_link:
            report_link = self.message_builder.create_context_block(
                [
                    f"<{model_runs_report_link.url}|{model_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        preview = self.message_builder.create_compacted_sections_blocks(
            [
                f"*Tags*\n{tags or '_No tags_'}",
                f"*Owners*\n{owners or '_No owners_'}",
                f"*Subscribers*\n{subscribers or '_No subscribers_'}",
            ]
        )

        result = []
        if alert.message:
            result.extend(
                [
                    self.message_builder.create_context_block(["*Result message*"]),
                    self.message_builder.create_text_section_block(
                        f"```{alert.message.strip()}```"
                    ),
                ]
            )

        configuration = []
        if alert.materialization:
            configuration.append(
                self.message_builder.create_context_block(["*Materialization*"])
            )
            configuration.append(
                self.message_builder.create_text_section_block(
                    f"`{str(alert.materialization)}`"
                )
            )
        if alert.full_refresh:
            configuration.append(
                self.message_builder.create_context_block(["*Full refresh*"])
            )
            configuration.append(
                self.message_builder.create_text_section_block(
                    f"`{alert.full_refresh}`"
                )
            )
        if alert.path:
            configuration.append(self.message_builder.create_context_block(["*Path*"]))
            configuration.append(
                self.message_builder.create_text_section_block(f"`{alert.path}`")
            )

        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_snapshot_template(
        self, alert: ModelAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        tags = self.message_builder.prettify_and_dedup_list(alert.tags)
        owners = self.message_builder.prettify_and_dedup_list(alert.owners)
        subscribers = self.message_builder.prettify_and_dedup_list(alert.subscribers)
        icon = self.message_builder.get_slack_status_icon(alert.status)

        title = [self.message_builder.create_header_block(f"{icon} dbt snapshot alert")]
        if alert.suppression_interval:
            title.extend(
                [
                    self.message_builder.create_context_block(
                        [
                            f"*Snapshot:* {alert.alias}     |",
                            f"*Status:* {alert.status}",
                        ],
                    ),
                    self.message_builder.create_context_block(
                        [
                            f"*Time:* {alert.detected_at_str}     |",
                            f"*Suppression interval:* {alert.suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.message_builder.create_context_block(
                    [
                        f"*Snapshot:* {alert.alias}     |",
                        f"*Status:* {alert.status}     |",
                        f"*{alert.detected_at_str}*",
                    ],
                ),
            )

        model_runs_report_link = get_model_runs_link(
            alert.report_url, alert.model_unique_id
        )
        if model_runs_report_link:
            report_link = self.message_builder.create_context_block(
                [
                    f"<{model_runs_report_link.url}|{model_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        preview = self.message_builder.create_compacted_sections_blocks(
            [
                f"*Tags*\n{tags or '_No tags_'}",
                f"*Owners*\n{owners or '_No owners_'}",
                f"*Subscribers*\n{subscribers or '_No subscribers_'}",
            ]
        )

        result = []
        if alert.message:
            result.extend(
                [
                    self.message_builder.create_context_block(["*Result message*"]),
                    self.message_builder.create_text_section_block(
                        f"```{alert.message.strip()}```"
                    ),
                ]
            )

        configuration = []
        if alert.original_path:
            configuration.append(self.message_builder.create_context_block(["*Path*"]))
            configuration.append(
                self.message_builder.create_text_section_block(
                    f"`{alert.original_path}`"
                )
            )

        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        tags = self.message_builder.prettify_and_dedup_list(alert.tags or [])
        owners = self.message_builder.prettify_and_dedup_list(alert.owners or [])
        subscribers = self.message_builder.prettify_and_dedup_list(
            alert.subscribers or []
        )
        icon = self.message_builder.get_slack_status_icon(alert.status)

        title = [
            self.message_builder.create_header_block(
                f"{icon} dbt source freshness alert"
            )
        ]
        if alert.suppression_interval:
            title.extend(
                [
                    self.message_builder.create_context_block(
                        [
                            f"*Source:* {alert.source_name}.{alert.identifier}     |",
                            f"*Status:* {alert.status}",
                        ],
                    ),
                    self.message_builder.create_context_block(
                        [
                            f"*Time:* {alert.detected_at_str}     |",
                            f"*Suppression interval:* {alert.suppression_interval} hours",
                        ],
                    ),
                ]
            )
        else:
            title.append(
                self.message_builder.create_context_block(
                    [
                        f"*Source:* {alert.source_name}.{alert.identifier}     |",
                        f"*Status:* {alert.status}     |",
                        f"*{alert.detected_at_str}*",
                    ],
                ),
            )

        test_runs_report_link = get_test_runs_link(
            alert.report_url, alert.source_freshness_execution_id
        )
        if test_runs_report_link:
            report_link = self.message_builder.create_context_block(
                [
                    f"<{test_runs_report_link.url}|{test_runs_report_link.text}>",
                ],
            )
            title.append(report_link)

        preview = self.message_builder.create_compacted_sections_blocks(
            [
                f"*Tags*\n{tags or '_No tags_'}",
                f"*Owners*\n{owners or '_No owners_'}",
                f"*Subscribers*\n{subscribers or '_No subscribers_'}",
            ]
        )

        if alert.freshness_description:
            preview.extend(
                [
                    self.message_builder.create_text_section_block("*Description*"),
                    self.message_builder.create_context_block(
                        [alert.freshness_description]
                    ),
                ]
            )
        else:
            preview.append(
                self.message_builder.create_text_section_block(
                    "*Description*\n_No description_"
                )
            )

        result = []
        if alert.status == "runtime error":
            result.extend(
                [
                    self.message_builder.create_context_block(["*Result message*"]),
                    self.message_builder.create_text_section_block(
                        f"Failed to calculate the source freshness\n"
                        f"```{alert.error}```"
                    ),
                ]
            )
        else:
            result.extend(
                [
                    self.message_builder.create_context_block(["*Result message*"]),
                    self.message_builder.create_text_section_block(
                        f"```{alert.result_description}```"
                    ),
                ]
            )
            result.extend(
                self.message_builder.create_compacted_sections_blocks(
                    [
                        f"*Time Elapsed*\n{timedelta(seconds=alert.max_loaded_at_time_ago_in_s) if alert.max_loaded_at_time_ago_in_s else 'N/A'}",
                        f"*Last Record At*\n{alert.max_loaded_at}",
                        f"*Sampled At*\n{alert.snapshotted_at_str}",
                    ]
                )
            )

        configuration = []

        if alert.error_after:
            configuration.append(
                self.message_builder.create_context_block(["*Error after*"])
            )
            configuration.append(
                self.message_builder.create_text_section_block(f"`{alert.error_after}`")
            )
        if alert.warn_after:
            configuration.append(
                self.message_builder.create_context_block(["*Warn after*"])
            )
            configuration.append(
                self.message_builder.create_text_section_block(f"`{alert.warn_after}`")
            )
        if alert.filter:
            configuration.append(
                self.message_builder.create_context_block(["*Filter*"])
            )
            configuration.append(
                self.message_builder.create_text_section_block(f"`{alert.filter}`")
            )
        if alert.path:
            configuration.append(self.message_builder.create_context_block(["*Path*"]))
            configuration.append(
                self.message_builder.create_text_section_block(f"`{alert.path}`")
            )

        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, *args, **kwargs
    ):
        alerts = alert.alerts
        model = alert.model

        title_blocks = []  # title, [banner], number of passed or failed,
        title_blocks.append(
            self.message_builder.create_header_block(
                f":small_red_triangle: Table issues detected - {model}"
            )
        )

        # summary of number of failed, errors, etc.
        fields_summary: List[str] = []
        # summary of number of failed, errors, etc.
        if alert.model_errors:
            fields_summary.append(f":X: Model errors: {len(alert.model_errors)}    |")
        if alert.test_failures:
            fields_summary.append(
                f":small_red_triangle: Test failures: {len(alert.test_failures)}    |"
            )
        if alert.test_warnings:
            fields_summary.append(
                f":warning: Test warnings: {len(alert.test_warnings)}    |"
            )
        if alert.test_errors:
            fields_summary.append(
                f":exclamation: Test errors: {len(alert.test_errors)}"
            )
        title_blocks.append(self.message_builder.create_context_block(fields_summary))

        report_link = None
        # No report link when there is only model error
        if not alert.model_errors:
            report_link = get_model_test_runs_link(
                alert.report_url, alert.model_unique_id
            )

        if report_link:
            report_link_block = self.message_builder.create_context_block(
                [
                    f"<{report_link.url}|{report_link.text}>",
                ],
            )
            title_blocks.append(report_link_block)

        self.message_builder.add_title_to_slack_alert(title_blocks=title_blocks)

        # attention required : tags, owners, subscribers
        preview_blocks = []

        tags = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.tags or [] for alert in alerts],
            prefix=self.message_builder._HASHTAG,
        )
        owners = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.owners or [] for alert in alerts]
        )
        subscribers = list_of_lists_of_strings_to_comma_delimited_unique_strings(
            [alert.subscribers or [] for alert in alerts]
        )
        preview_blocks.append(
            self.message_builder.create_text_section_block(
                f"*Tags*: {tags if tags else '_No tags_'}"
            )
        )
        preview_blocks.append(
            self.message_builder.create_text_section_block(
                f"*Owners*: {owners if owners else '_No owners_'}"
            )
        )
        preview_blocks.append(
            self.message_builder.create_text_section_block(
                f"*Subscribers*: {subscribers if subscribers else '_No subscribers_'}"
            )
        )
        self.message_builder.add_preview_to_slack_alert(preview_blocks=preview_blocks)

        details_blocks = []
        # Model errors
        if alert.model_errors:
            details_blocks.append(
                self.message_builder.create_text_section_block("*Model errors*")
            )
            details_blocks.append(self.message_builder.create_divider_block())
            block_header = self.message_builder.create_context_block(
                self._get_model_error_block_header(alert.model_errors)
            )
            block_body = self.message_builder.create_text_section_block(
                self._get_model_error_block_body(alert.model_errors)
            )
            details_blocks.extend([block_header, block_body])

        # Test failures
        if alert.test_failures:
            details_blocks.append(
                self.message_builder.create_text_section_block("*Test failures*")
            )
            rows = [alert.concise_name for alert in alert.test_failures]
            text = "\n".join([f":small_red_triangle: {row}" for row in rows])
            details_blocks.append(self.message_builder.create_text_section_block(text))

        # Test warnings
        if alert.test_warnings:
            details_blocks.append(
                self.message_builder.create_text_section_block("*Test warnings*")
            )
            rows = [alert.concise_name for alert in alert.test_warnings]
            text = "\n".join([f":warning: {row}" for row in rows])
            details_blocks.append(self.message_builder.create_text_section_block(text))

        # Test errors
        if alert.test_errors:
            details_blocks.append(
                self.message_builder.create_text_section_block("*Test errors*")
            )
            rows = [alert.concise_name for alert in alert.test_errors]
            text = "\n".join([f":exclamation: {row}" for row in rows])
            details_blocks.append(self.message_builder.create_text_section_block(text))

        self.message_builder._add_blocks_as_attachments(details_blocks)
        return self.message_builder.get_slack_message()

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
    ) -> SlackMessageSchema:
        return SlackMessageSchema(
            text=self.message_builder.get_limited_markdown_msg(
                f":small_red_triangle: Oops, we failed to format the alert :confused:\n"
                f"Please share this with the Elementary team via <https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg|Slack> or a <https://github.com/elementary-data/elementary/issues/new|GitHub> issue.\n"
                f"```{json.dumps(alert.data, indent=2)}```"
            )
        )

    def _get_test_message_template(self, *args, **kwargs) -> SlackMessageSchema:
        return SlackMessageSchema(
            text=f"Elementary monitor ran successfully on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )

    def _parse_emails_to_ids(self, emails: Optional[List[str]]) -> List[str]:
        def _regex_match_owner_email(potential_email: str) -> bool:
            email_regex = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"

            return bool(re.fullmatch(email_regex, potential_email))

        def _get_user_id(email: str) -> str:
            user_id = self.client.get_user_id_from_email(email)
            return f"<@{user_id}>" if user_id else email

        if emails is None:
            return []

        return [
            _get_user_id(email) if _regex_match_owner_email(email) else email
            for email in emails
        ]

    def _fix_owners_and_subscribers(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
    ):
        if isinstance(alert, GroupedByTableAlerts):
            for grouped_alert in alert.alerts:
                grouped_alert.owners = self._parse_emails_to_ids(grouped_alert.owners)
                grouped_alert.subscribers = self._parse_emails_to_ids(
                    grouped_alert.subscribers
                )
        else:
            alert.owners = self._parse_emails_to_ids(alert.owners)
            alert.subscribers = self._parse_emails_to_ids(alert.subscribers)

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
        integration_params = self._get_integration_params(alert=alert)
        channel_name = integration_params.get("channel")
        try:
            self._fix_owners_and_subscribers(alert)
            template = self._get_alert_template(alert)
            sent_successfully = self.client.send_message(
                channel_name=channel_name, message=template
            )
        except Exception:
            sent_successfully = False

        if not sent_successfully:
            try:
                fallback_template = self._get_fallback_template(alert)
                fallback_sent_successfully = self.client.send_message(
                    channel_name=channel_name, message=fallback_template
                )
            except Exception:
                fallback_sent_successfully = False
            self.message_builder.reset_slack_message()
            return fallback_sent_successfully

        self.message_builder.reset_slack_message()
        return sent_successfully

    def send_test_message(self, channel_name: str, *args, **kwargs) -> bool:
        test_message = self._get_test_message_template()
        return self.client.send_message(channel_name=channel_name, message=test_message)

    def _get_integration_params(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
            GroupedByTableAlerts,
        ],
        *args,
        **kwargs,
    ) -> Dict[str, Any]:
        integration_params = dict()
        if isinstance(self.client, SlackWebClient):
            integration_params.update(
                dict(
                    channel=(
                        self.config.slack_channel_name
                        if self.override_config_defaults
                        else (
                            alert.unified_meta.get("channel")
                            or self.config.slack_channel_name
                        )
                    )
                )
            )
        return integration_params
