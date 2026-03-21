import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Union

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.client import SlackClient, SlackWebClient
from elementary.clients.slack.schema import SlackBlocksType, SlackMessageSchema
from elementary.clients.slack.slack_message_builder import MessageColor
from elementary.config.config import Config
from elementary.monitor.alerts.alerts_groups import AlertsGroup, GroupedByTableAlerts
from elementary.monitor.alerts.alerts_groups.base_alerts_group import BaseAlertsGroup
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.slack.message_builder import (
    SlackAlertMessageBuilder,
    SlackAlertMessageSchema,
)
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    get_model_test_runs_link,
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
    "fail": {"color": MessageColor.RED, "display_name": "Failure"},
    "warn": {"color": MessageColor.YELLOW, "display_name": "Warning"},
    "error": {"color": MessageColor.RED, "display_name": "Error"},
}


class SlackIntegration(BaseIntegration):
    COMPACT_SCHEMA_THRESHOLD = 500

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
            BaseAlertsGroup,
        ],
        *args,
        **kwargs,
    ) -> SlackMessageSchema:
        if self.config.is_slack_workflow:
            return SlackMessageSchema(text=json.dumps(alert.data, sort_keys=True))
        alert_schema = super()._get_alert_template(alert, *args, **kwargs)
        return self.message_builder.get_slack_message(alert_schema=alert_schema)

    def _get_dbt_test_template(
        self, alert: TestAlertModel, *args, **kwargs
    ) -> SlackAlertMessageSchema:
        self.message_builder.add_message_color(self._get_color(alert.status))
        title = [
            self.message_builder.create_header_block(
                f"{self._get_display_name(alert.status)}: {alert.summary}"
            )
        ]
        if alert.suppression_interval:
            title.extend(
                [
                    self.message_builder.create_context_block(
                        [
                            f"*Test:* {alert.concise_name}     |",
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
                        f"*Test:* {alert.concise_name}     |",
                        f"*Status:* {alert.status}     |",
                        f"*{alert.detected_at_str}*",
                    ],
                ),
            )

        test_runs_report_link = alert.get_report_link()

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
            tags = prettify_and_dedup_list(alert.tags or [])
            compacted_sections.append(f"*Tags*\n{tags or '_No tags_'}")
        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            compacted_sections.append(f"*Owners*\n{owners or '_No owners_'}")
        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
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

        return SlackAlertMessageSchema(
            title=title,
            preview=preview,
            details=self._create_single_alert_details_blocks(
                result=result, configuration=configuration
            ),
        )

    def _get_elementary_test_template(
        self, alert: TestAlertModel, *args, **kwargs
    ) -> SlackAlertMessageSchema:
        self.message_builder.add_message_color(self._get_color(alert.status))

        anomalous_value = (
            alert.other if alert.test_type == "anomaly_detection" else None
        )

        title = [
            self.message_builder.create_header_block(
                f"{alert.summary}"
                if alert.test_type == "schema_change"
                else f"{self._get_display_name(alert.status)}: {alert.summary}"
            ),
        ]
        if alert.suppression_interval:
            title.extend(
                [
                    self.message_builder.create_context_block(
                        [
                            f"*Test:* {alert.concise_name}     |",
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
                        f"*Test:* {alert.concise_name}     |",
                        f"*Status:* {alert.status}     |",
                        f"*{alert.detected_at_str}*",
                    ],
                ),
            )

        test_runs_report_link = alert.get_report_link()

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
            tags = prettify_and_dedup_list(alert.tags or [])
            compacted_sections.append(f"*Tags*\n{tags or '_No tags_'}")
        if OWNERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            owners = prettify_and_dedup_list(alert.owners or [])
            compacted_sections.append(f"*Owners*\n{owners or '_No owners_'}")
        if SUBSCRIBERS_FIELD in (alert.alert_fields or DEFAULT_ALERT_FIELDS):
            subscribers = prettify_and_dedup_list(alert.subscribers or [])
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

        return SlackAlertMessageSchema(
            title=title,
            preview=preview,
            details=self._create_single_alert_details_blocks(
                result=result, configuration=configuration
            ),
        )

    def _get_model_template(
        self, alert: ModelAlertModel, *args, **kwargs
    ) -> SlackAlertMessageSchema:
        tags = prettify_and_dedup_list(alert.tags)
        owners = prettify_and_dedup_list(alert.owners)
        subscribers = prettify_and_dedup_list(alert.subscribers)
        self.message_builder.add_message_color(self._get_color(alert.status))

        title = [
            self.message_builder.create_header_block(
                f"{self._get_display_name(alert.status)}: {alert.summary}"
            )
        ]
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

        model_runs_report_link = alert.get_report_link()

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

        return SlackAlertMessageSchema(
            title=title,
            preview=preview,
            details=self._create_single_alert_details_blocks(
                result=result, configuration=configuration
            ),
        )

    def _get_snapshot_template(
        self, alert: ModelAlertModel, *args, **kwargs
    ) -> SlackAlertMessageSchema:
        tags = prettify_and_dedup_list(alert.tags)
        owners = prettify_and_dedup_list(alert.owners)
        subscribers = prettify_and_dedup_list(alert.subscribers)
        self.message_builder.add_message_color(self._get_color(alert.status))

        title = [
            self.message_builder.create_header_block(
                f"{self._get_display_name(alert.status)}: {alert.summary}"
            )
        ]
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

        model_runs_report_link = alert.get_report_link()

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

        return SlackAlertMessageSchema(
            title=title,
            preview=preview,
            details=self._create_single_alert_details_blocks(
                result=result, configuration=configuration
            ),
        )

    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, *args, **kwargs
    ) -> SlackAlertMessageSchema:
        tags = prettify_and_dedup_list(alert.tags or [])
        owners = prettify_and_dedup_list(alert.owners or [])
        subscribers = prettify_and_dedup_list(alert.subscribers or [])
        self.message_builder.add_message_color(self._get_color(alert.status))
        title = [
            self.message_builder.create_header_block(
                f"{self._get_display_name(alert.status)}: {alert.summary}"
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

        test_runs_report_link = alert.get_report_link()

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

        return SlackAlertMessageSchema(
            title=title,
            preview=preview,
            details=self._create_single_alert_details_blocks(
                result=result, configuration=configuration
            ),
        )

    def _get_alert_type_counters_block(self, alert: AlertsGroup) -> dict:
        counters_texts: List[str] = []
        if alert.model_errors:
            counters_texts.append(f":X: Model errors: {len(alert.model_errors)}")
        if alert.test_failures:
            counters_texts.append(
                f":small_red_triangle: Test failures: {len(alert.test_failures)}"
            )
        if alert.test_warnings:
            counters_texts.append(
                f":warning: Test warnings: {len(alert.test_warnings)}"
            )
        if alert.test_errors:
            counters_texts.append(
                f":exclamation: Test errors: {len(alert.test_errors)}"
            )
        return self.message_builder.create_context_block(
            ["    |    ".join(counters_texts)]
        )

    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, *args, **kwargs
    ) -> SlackAlertMessageSchema:
        alerts = alert.alerts

        self.message_builder.add_message_color(self._get_color(alert.status))

        title_blocks = [
            self.message_builder.create_header_block(
                f"{self._get_display_name(alert.status)}: {alert.summary}"
            ),
            self._get_alert_type_counters_block(alert),
        ]

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

        return SlackAlertMessageSchema(
            title=title_blocks, preview=preview_blocks, details=details_blocks
        )

    def _add_compact_sub_group_details_block(
        self,
        details_blocks: list,
        alerts: Sequence[
            Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                GroupedByTableAlerts,
            ],
        ],
        sub_title: str,
        bullet_icon: str,
    ) -> None:
        if not alerts:
            return
        details_blocks.append(
            self.message_builder.create_text_section_block(
                f":{bullet_icon}: {len(alerts)} {sub_title}"
            )
        )

    def _get_compact_sub_group_details_block(
        self, alert: AlertsGroup, *args, **kwargs
    ) -> List[dict]:
        details_blocks: List[dict] = []
        self._add_compact_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.model_errors,
            sub_title="Model Errors",
            bullet_icon="X",
        )
        self._add_compact_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.test_failures,
            sub_title="Test Failures",
            bullet_icon="small_red_triangle",
        )
        self._add_compact_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.test_warnings,
            sub_title="Test Warnings",
            bullet_icon="warning",
        )
        self._add_compact_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.test_errors,
            sub_title="Test Errors",
            bullet_icon="exclamation",
        )
        return details_blocks

    def _get_alerts_group_compact_template(
        self, alert: AlertsGroup
    ) -> SlackAlertMessageSchema:
        self.message_builder.add_message_color(self._get_color(alert.status))

        title_blocks = [
            self.message_builder.create_header_block(
                f"{self._get_display_name(alert.status)}: {alert.summary}"
            )
        ]

        details_blocks = self._get_compact_sub_group_details_block(alert)
        return SlackAlertMessageSchema(title=title_blocks, details=details_blocks)

    def _add_sub_group_details_block(
        self,
        details_blocks: list,
        alerts: Sequence[
            Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
                GroupedByTableAlerts,
            ],
        ],
        sub_title: str,
        bullet_icon: str,
    ) -> None:
        if not alerts:
            return

        section_text_rows = [f"*{sub_title}*"]
        for alert in alerts:
            text = f":{bullet_icon}: {alert.summary}"
            if report_link := alert.get_report_link():
                text = " - ".join([text, f"<{report_link.url}|{report_link.text}>"])
            section_text_rows.append(text)

        section = self.message_builder.create_text_section_block(
            "\n".join(section_text_rows)
        )
        details_blocks.append(section)

    def _get_sub_group_details_blocks(
        self, alert: AlertsGroup, *args, **kwargs
    ) -> List[dict]:
        details_blocks: List[dict] = []
        self._add_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.model_errors,
            sub_title="Model Errors",
            bullet_icon="X",
        )
        self._add_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.test_failures,
            sub_title="Test Failures",
            bullet_icon="small_red_triangle",
        )
        self._add_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.test_warnings,
            sub_title="Test Warnings",
            bullet_icon="warning",
        )
        self._add_sub_group_details_block(
            details_blocks=details_blocks,
            alerts=alert.test_errors,
            sub_title="Test Errors",
            bullet_icon="exclamation",
        )
        return details_blocks

    def _get_alerts_group_template(
        self, alert: BaseAlertsGroup, *args, **kwargs
    ) -> SlackAlertMessageSchema:
        if len(alert.alerts) >= self.COMPACT_SCHEMA_THRESHOLD:
            return self._get_alerts_group_compact_template(alert)  # type: ignore[arg-type]

        self.message_builder.add_message_color(self._get_color(alert.status))
        title_blocks = [
            self.message_builder.create_header_block(
                f"{self._get_display_name(alert.status)}: {alert.summary}"
            ),
            self._get_alert_type_counters_block(alert),  # type: ignore[arg-type]
        ]
        details_blocks = self._get_sub_group_details_blocks(alert)  # type: ignore[arg-type]
        return SlackAlertMessageSchema(title=title_blocks, details=details_blocks)

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
            BaseAlertsGroup,
        ],
        *args,
        **kwargs,
    ) -> SlackMessageSchema:
        return SlackMessageSchema(
            text=self.message_builder.get_limited_markdown_msg(
                f":small_red_triangle: Oops, we failed to format the alert :confused:\n"
                f"Please share this with the Elementary team via <https://www.elementary-data.com/community|Slack> or a <https://github.com/elementary-data/elementary/issues/new|GitHub> issue.\n"
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
            BaseAlertsGroup,
        ],
    ):
        if isinstance(alert, BaseAlertsGroup):
            for inner_alert in alert.alerts:
                inner_alert.owners = self._parse_emails_to_ids(inner_alert.owners)
                inner_alert.subscribers = self._parse_emails_to_ids(
                    inner_alert.subscribers
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
            BaseAlertsGroup,
        ],
        *args,
        **kwargs,
    ) -> bool:
        integration_params = self._get_integration_params(alert=alert)
        channel_name = integration_params.get("channel")
        logger.debug(f"Sending alert to Slack channel: {channel_name}")
        try:
            self._fix_owners_and_subscribers(alert)
            template = self._get_alert_template(alert)
            sent_successfully = self.client.send_message(
                channel_name=channel_name, message=template
            )
        except Exception as err:
            logger.error(
                f"Unable to send alert via Slack: {err}\nSending fallback template."
            )
            sent_successfully = False

        if (
            not sent_successfully
            and self.config.slack_channel_name
            and channel_name != self.config.slack_channel_name
        ):
            try:
                logger.debug(
                    f"Sending alert to default Slack channel: {self.config.slack_channel_name}"
                )
                channel_name = self.config.slack_channel_name
                sent_successfully = self.client.send_message(
                    channel_name=channel_name, message=template
                )
            except Exception as err:
                logger.error(
                    f"Unable to send alert via Slack to default channel: {err}"
                )
                sent_successfully = False

        if not sent_successfully:
            try:
                fallback_template = self._get_fallback_template(alert)
                fallback_sent_successfully = self.client.send_message(
                    channel_name=channel_name, message=fallback_template
                )
            except Exception as err:
                logger.error(f"Unable to send alert fallback via Slack: {err}")
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
            BaseAlertsGroup,
        ],
        *args,
        **kwargs,
    ) -> Dict[str, Any]:
        integration_params = dict()
        if isinstance(self.client, SlackWebClient):
            if self.override_config_defaults:
                channel = self.config.slack_channel_name
                logger.debug(f"Using override config default channel: {channel}")
            elif alert.unified_meta.get("channel"):
                channel = alert.unified_meta.get("channel")
                logger.debug(f"Using channel from meta: {channel}")
            else:
                channel = self.config.slack_channel_name
            integration_params.update({"channel": channel})
        return integration_params

    def _create_single_alert_details_blocks(
        self, result: SlackBlocksType, configuration: SlackBlocksType
    ) -> SlackBlocksType:
        details_blocks = []
        if result:
            details_blocks.extend(
                [
                    self.message_builder.create_text_section_block(":mag: *Result*"),
                    self.message_builder.create_divider_block(),
                    *result,
                ]
            )
        if configuration:
            details_blocks.extend(
                [
                    self.message_builder.create_text_section_block(
                        ":hammer_and_wrench: *Configuration*"
                    ),
                    self.message_builder.create_divider_block(),
                    *configuration,
                ]
            )
        return details_blocks

    @staticmethod
    def _get_display_name(alert_status: Optional[str]) -> str:
        if alert_status is None:
            return "Unknown"
        return STATUS_DISPLAYS.get(alert_status, {}).get("display_name", alert_status)

    @staticmethod
    def _get_color(alert_status: Optional[str]) -> MessageColor:
        if alert_status is None:
            return MessageColor.RED
        return STATUS_DISPLAYS.get(alert_status, {}).get("color", MessageColor.RED)
