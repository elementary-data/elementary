import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from slack_sdk.models.blocks import SectionBlock

from elementary.clients.slack.client import SlackClient, SlackWebClient
from elementary.clients.slack.schema import SlackMessageSchema
from elementary.config.config import Config
from elementary.monitor.alerts.alert import (
    COLUMN_FIELD,
    OWNERS_FIELD,
    RED,
    RESULT_MESSAGE_FIELD,
    SUBSCRIBERS_FIELD,
    TABLE_FIELD,
    TAGS_FIELD,
    TEST_PARAMS_FIELD,
    TEST_QUERY_FIELD,
    TEST_RESULTS_SAMPLE_FIELD,
    YELLOW,
)
from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import (
    TEST_QUERY_QUERY_TEMPLATE,
    TestAlertModel,
)
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)
from elementary.monitor.data_monitoring.alerts.integrations.slack.message_builder import (
    SlackAlertMessageBuilder,
)
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.json_utils import (
    list_of_lists_of_strings_to_comma_delimited_unique_strings,
)


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
        self.message_builder.add_alert_color(alert)
        title = self.message_builder.get_alert_title(alert)
        preview = self.message_builder.get_compact_sections_for_alert(
            alert,
            {
                TABLE_FIELD: alert.table_full_name,
                COLUMN_FIELD: alert.column_name,
                TAGS_FIELD: alert.tags,
                OWNERS_FIELD: alert.owners,
                SUBSCRIBERS_FIELD: alert.subscribers,
            },
        )

        preview.extend(
            self.message_builder.get_description_blocks(alert.test_description)
        )

        result_fields = {
            RESULT_MESSAGE_FIELD: alert.error_message,
            TEST_RESULTS_SAMPLE_FIELD: alert.test_rows_sample,
        }

        if alert.test_results_query:
            if len(alert.test_results_query) < SectionBlock.text_max_length:
                result_fields[TEST_QUERY_FIELD] = alert.test_results_query
            else:
                result_fields[TEST_QUERY_FIELD] = TEST_QUERY_QUERY_TEMPLATE.format(
                    db_and_schema=alert.elementary_database_and_schema,
                    alert_id=alert.id,
                )

        result = self.message_builder.get_extended_sections_for_alert(
            alert, result_fields
        )

        configuration = self.message_builder.get_extended_sections_for_alert(
            alert, {TEST_PARAMS_FIELD: alert.test_params}
        )
        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_elementary_test_template(
        self, alert: TestAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        self.message_builder.add_alert_color(alert)
        title = self.message_builder.get_alert_title(alert)
        preview = self.message_builder.get_compact_sections_for_alert(
            alert,
            {
                TABLE_FIELD: alert.table_full_name,
                COLUMN_FIELD: alert.column_name,
                TAGS_FIELD: alert.tags,
                OWNERS_FIELD: alert.owners,
                SUBSCRIBERS_FIELD: alert.subscribers,
            },
        )

        preview.extend(
            self.message_builder.get_description_blocks(alert.test_description)
        )

        result_fields = {
            RESULT_MESSAGE_FIELD: alert.error_message,
            TEST_RESULTS_SAMPLE_FIELD: alert.other
            if alert.test_type == "anomaly_detection"
            else None,
        }
        result = self.message_builder.get_extended_sections_for_alert(
            alert, result_fields
        )

        configuration = self.message_builder.get_extended_sections_for_alert(
            alert, {TEST_PARAMS_FIELD: alert.test_params}
        )
        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_model_template(
        self, alert: ModelAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        self.message_builder.add_alert_color(alert)
        title = self.message_builder.get_alert_title(alert)
        preview = self.message_builder.get_compact_sections_for_alert(
            alert,
            {
                TAGS_FIELD: alert.tags,
                OWNERS_FIELD: alert.owners,
                SUBSCRIBERS_FIELD: alert.subscribers,
            },
        )

        result = self.message_builder.get_extended_sections_for_alert(
            alert, {RESULT_MESSAGE_FIELD: alert.message}
        )

        configuration = self.message_builder.get_extended_sections_for_alert(
            alert,
            {
                "Materialization": alert.materialization,
                "Full refresh": alert.full_refresh,
                "Path": alert.path,
            },
        )
        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_snapshot_template(
        self, alert: ModelAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        self.message_builder.add_alert_color(alert)
        title = self.message_builder.get_alert_title(alert)
        preview = self.message_builder.get_compact_sections_for_alert(
            alert,
            {
                TAGS_FIELD: alert.tags,
                OWNERS_FIELD: alert.owners,
                SUBSCRIBERS_FIELD: alert.subscribers,
            },
        )
        result = self.message_builder.get_extended_sections_for_alert(
            alert, {RESULT_MESSAGE_FIELD: alert.message}
        )

        configuration = self.message_builder.get_extended_sections_for_alert(
            alert,
            {
                "Path": alert.original_path,
            },
        )

        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_source_freshness_template(
        self, alert: SourceFreshnessAlertModel, *args, **kwargs
    ) -> SlackMessageSchema:
        self.message_builder.add_alert_color(alert)
        title = self.message_builder.get_alert_title(alert)
        preview = self.message_builder.get_compact_sections_for_alert(
            alert,
            {
                TAGS_FIELD: alert.tags,
                OWNERS_FIELD: alert.owners,
                SUBSCRIBERS_FIELD: alert.subscribers,
            },
        )

        preview.extend(
            self.message_builder.get_description_blocks(alert.freshness_description)
        )

        if alert.status == "runtime error":
            result = self.message_builder.get_extended_sections_for_alert(
                alert,
                {
                    RESULT_MESSAGE_FIELD: f"Failed to calculate the source freshness\n```{alert.error}```"
                },
            )
        else:
            result = self.message_builder.get_extended_sections_for_alert(
                alert,
                {
                    RESULT_MESSAGE_FIELD: alert.result_description,
                },
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

        configuration = self.message_builder.get_extended_sections_for_alert(
            alert,
            {
                "Error after": alert.error_after,
                "Warn after": alert.warn_after,
                "Filter": alert.filter,
                "Path": alert.path,
            },
        )

        return self.message_builder.get_slack_message(
            title=title, preview=preview, result=result, configuration=configuration
        )

    def _get_group_by_table_template(
        self, alert: GroupedByTableAlerts, *args, **kwargs
    ):
        alerts = alert.alerts
        alert_color = (
            YELLOW if all([_alert.status == "warn" for _alert in alerts]) else RED
        )
        self.message_builder.add_color_to_slack_alert(alert_color)

        title_blocks = [self.message_builder.create_header_block(alert.summary)]

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

        report_link = alert.get_report_link()
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
        preview_blocks = self.message_builder.get_compact_sections_for_alert(
            alert,
            {TAGS_FIELD: tags, OWNERS_FIELD: owners, SUBSCRIBERS_FIELD: subscribers},
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
            rows = [alert.summary for alert in alert.test_failures]
            text = "\n".join([f":small_red_triangle: {row}" for row in rows])
            details_blocks.append(self.message_builder.create_text_section_block(text))

        # Test warnings
        if alert.test_warnings:
            details_blocks.append(
                self.message_builder.create_text_section_block("*Test warnings*")
            )
            rows = [alert.summary for alert in alert.test_warnings]
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
