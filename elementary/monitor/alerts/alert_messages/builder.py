from datetime import timedelta
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import BaseModel

from elementary.messages.block_builders import (
    BoldTextLineBlock,
    BulletListBlock,
    FactsBlock,
    ItalicTextLineBlock,
    JsonCodeBlock,
    LinksLineBlock,
    MentionLineBlock,
    NonPrimaryFactBlock,
    PrimaryFactBlock,
    SummaryLineBlock,
    TextLineBlock,
)
from elementary.messages.blocks import (
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    FactListBlock,
    HeaderBlock,
    Icon,
    InlineBlock,
    InlineCodeBlock,
    LineBlock,
    LinesBlock,
    LinkBlock,
    TableBlock,
    TextBlock,
    TextStyle,
)
from elementary.messages.message_body import Color, MessageBlock, MessageBody
from elementary.monitor.alerts.alert_messages.alert_fields import AlertField
from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.alerts_groups.base_alerts_group import BaseAlertsGroup
from elementary.monitor.alerts.alerts_groups.grouped_by_table import (
    GroupedByTableAlerts,
)
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
)

AlertType = Union[
    TestAlertModel,
    ModelAlertModel,
    SourceFreshnessAlertModel,
    BaseAlertsGroup,
]


class MessageBuilderConfig(BaseModel):
    alert_groups_subscribers: bool = False


class AlertMessageBuilder:
    def __init__(self, config: Optional[MessageBuilderConfig] = None):
        self.config = config or MessageBuilderConfig()

    STATUS_DISPLAYS: Dict[str, str] = {
        "fail": "Failure",
        "warn": "Warning",
        "error": "Error",
        "runtime error": "Runtime Error",
    }

    STATUS_COLORS: Dict[str, Color] = {
        "fail": Color.RED,
        "warn": Color.YELLOW,
        "error": Color.RED,
        "runtime error": Color.RED,
    }

    def _get_display_name(self, status: Optional[str]) -> Optional[str]:
        if status is None:
            return None
        return self.STATUS_DISPLAYS.get(status, status.capitalize())

    def _get_color(self, alert_status: Optional[str]) -> Optional[Color]:
        if alert_status is None:
            return None
        return self.STATUS_COLORS.get(alert_status)

    def _get_alert_color(self, alert: AlertType) -> Optional[Color]:
        return self._get_color(alert.status)

    def _get_alert_title(
        self, summary: str, status: Optional[str], test_type: Optional[str]
    ) -> str:
        if test_type == "schema_change":
            return summary
        return f"{self._get_display_name(status)}: {summary}" if status else summary

    def _get_run_alert_subtitle_block(
        self,
        type: Literal["test", "snapshot", "model", "source"],
        name: str,
        status: Optional[str] = None,
        detected_at_str: Optional[str] = None,
        suppression_interval: Optional[int] = None,
        env: Optional[str] = None,
        links: list[ReportLinkData] = [],
    ) -> LinesBlock:
        summary = []
        summary.append((type.capitalize() + ":", name))
        if env:
            summary.append(("Env:", env))
        summary.append(("Status:", status or "Unknown"))
        if detected_at_str:
            summary.append(("Time:", detected_at_str))
        if suppression_interval:
            summary.append(("Suppression interval:", str(suppression_interval)))
        subtitle_lines = [SummaryLineBlock(summary=summary)]

        if links:
            subtitle_lines.append(
                LinksLineBlock(
                    links=[(link.text, link.url, link.icon) for link in links]
                )
            )
        return LinesBlock(lines=subtitle_lines)

    def _get_run_alert_subtitle_links(
        self,
        alert: Union[TestAlertModel, SourceFreshnessAlertModel, ModelAlertModel],
    ) -> List[ReportLinkData]:
        report_link = alert.get_report_link()
        if report_link:
            return [report_link]
        return []

    def _get_run_alert_subtitle_blocks(
        self,
        alert: Union[TestAlertModel, SourceFreshnessAlertModel, ModelAlertModel],
    ) -> List[MessageBlock]:
        asset_type: Literal["test", "snapshot", "model", "source"]
        asset_name: str
        if isinstance(alert, TestAlertModel):
            asset_type = "test"
            asset_name = alert.concise_name
        elif isinstance(alert, SourceFreshnessAlertModel):
            asset_type = "source"
            asset_name = f"{alert.source_name}.{alert.identifier}"
        elif isinstance(alert, ModelAlertModel):
            asset_type = "snapshot" if alert.materialization == "snapshot" else "model"
            asset_name = alert.alias
        links = self._get_run_alert_subtitle_links(alert)
        return [
            self._get_run_alert_subtitle_block(
                type=asset_type,
                name=asset_name,
                status=alert.status,
                detected_at_str=alert.detected_at_str,
                suppression_interval=alert.suppression_interval,
                env=alert.env,
                links=links,
            )
        ]

    def _get_alert_counters_subtitle_block(
        self,
        model_errors_count: int = 0,
        test_failures_count: int = 0,
        test_warnings_count: int = 0,
        test_errors_count: int = 0,
        env: Optional[str] = None,
    ) -> LinesBlock:
        summary: List[Tuple[Union[Tuple[Icon, str], str], str]] = []
        if model_errors_count:
            summary.append(((Icon.X, "Model Errors:"), str(model_errors_count)))
        if test_failures_count:
            summary.append(
                ((Icon.RED_TRIANGLE, "Test Failures:"), str(test_failures_count))
            )
        if test_warnings_count:
            summary.append(((Icon.WARNING, "Test Warnings:"), str(test_warnings_count)))
        if test_errors_count:
            summary.append(((Icon.EXCLAMATION, "Test Errors:"), str(test_errors_count)))
        if env:
            summary.append(("Env:", env))
        subtitle_lines = [SummaryLineBlock(summary=summary)]

        return LinesBlock(lines=subtitle_lines)

    def _get_alerts_group_subtitle_blocks(
        self,
        alert: Union[AlertsGroup, GroupedByTableAlerts],
    ) -> List[MessageBlock]:
        return [
            self._get_alert_counters_subtitle_block(
                model_errors_count=len(alert.model_errors),
                test_failures_count=len(alert.test_failures),
                test_warnings_count=len(alert.test_warnings),
                test_errors_count=len(alert.test_errors),
                env=alert.env,
            )
        ]

    def _get_details_blocks(
        self,
        table: Optional[str] = None,
        column: Optional[str] = None,
        tags: Optional[List[str]] = None,
        owners: Optional[List[str]] = None,
        subscribers: Optional[List[str]] = None,
        description: Optional[str] = None,
        path: Optional[str] = None,
        fields: List[str] = [],
    ) -> List[MessageBlock]:
        tags = sorted(list(set(tags))) if tags else None
        owners = sorted(list(set(owners))) if owners else None
        subscribers = sorted(list(set(subscribers))) if subscribers else None

        blocks: List[MessageBlock] = []
        if not (
            table or column or tags or owners or subscribers or description or path
        ):
            return blocks
        blocks.append(
            LinesBlock(
                lines=[
                    BoldTextLineBlock(text=[Icon.INFO, "Details"]),
                ]
            )
        )
        fact_blocks = []
        if table and AlertField.TABLE in fields:
            fact_blocks.append(
                PrimaryFactBlock(
                    (TextLineBlock(text="Table"), TextLineBlock(text=table))
                )
            )
        if column and AlertField.COLUMN in fields:
            fact_blocks.append(
                NonPrimaryFactBlock(
                    (TextLineBlock(text="Column"), TextLineBlock(text=column))
                )
            )

        tags_line = (
            TextLineBlock(text=", ".join(tags))
            if tags
            else ItalicTextLineBlock(text="No tags")
        )
        owners_line = (
            MentionLineBlock(*owners)
            if owners
            else ItalicTextLineBlock(text="No owners")
        )
        subscribers_line = (
            MentionLineBlock(*subscribers)
            if subscribers
            else ItalicTextLineBlock(text="No subscribers")
        )
        if AlertField.TAGS in fields:
            fact_blocks.append(
                NonPrimaryFactBlock((TextLineBlock(text="Tags"), tags_line))
            )
        if AlertField.OWNERS in fields:
            fact_blocks.append(
                NonPrimaryFactBlock((TextLineBlock(text="Owners"), owners_line))
            )
        if AlertField.SUBSCRIBERS in fields:
            fact_blocks.append(
                NonPrimaryFactBlock(
                    (TextLineBlock(text="Subscribers"), subscribers_line)
                )
            )

        if description and AlertField.DESCRIPTION in fields:
            fact_blocks.append(
                PrimaryFactBlock(
                    (TextLineBlock(text="Description"), TextLineBlock(text=description))
                )
            )
        if path:
            fact_blocks.append(
                PrimaryFactBlock(
                    (
                        TextLineBlock(text="Path"),
                        LineBlock(inlines=[InlineCodeBlock(code=path)]),
                    )
                )
            )
        blocks.append(FactListBlock(facts=fact_blocks))
        return blocks

    def _get_result_blocks(
        self,
        result_message: Optional[str],
        result_sample: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]] = None,
        result_query: Optional[str] = None,
        anomalous_value: Optional[dict] = None,
        time_elapsed: Optional[str] = None,
        last_record_at: Optional[str] = None,
        sampled_at: Optional[str] = None,
        fields: List[str] = [],
    ) -> List[MessageBlock]:
        result_blocks: List[MessageBlock] = []
        if result_message and AlertField.RESULT_MESSAGE in fields:
            result_blocks.append(
                LinesBlock(
                    lines=[
                        BoldTextLineBlock(text="Result Message"),
                    ]
                )
            )
            result_blocks.append(
                CodeBlock(text=result_message.strip()),
            )
        if (
            result_sample or anomalous_value
        ) and AlertField.TEST_RESULTS_SAMPLE in fields:
            result_blocks.append(
                LinesBlock(
                    lines=[
                        BoldTextLineBlock(
                            text=[Icon.MAGNIFYING_GLASS, "Test Results Sample"]
                        ),
                    ]
                )
            )
            if anomalous_value:
                result_blocks.append(
                    LinesBlock(
                        lines=[
                            LineBlock(
                                inlines=[
                                    TextBlock(
                                        text="Anomalous Value:", style=TextStyle.BOLD
                                    ),
                                    TextBlock(text=str(anomalous_value)),
                                ]
                            ),
                        ]
                    )
                )
            elif result_sample:
                if (
                    isinstance(result_sample, list)
                    and len(result_sample[0].keys()) <= 4
                ):
                    result_blocks.append(
                        TableBlock.from_dicts(result_sample),
                    )
                else:
                    result_blocks.append(
                        JsonCodeBlock(content=result_sample),
                    )
        if result_query and AlertField.TEST_QUERY in fields:
            result_blocks.append(
                LinesBlock(
                    lines=[
                        BoldTextLineBlock(text=["Test Results Query"]),
                    ]
                )
            )
            result_blocks.append(CodeBlock(text=result_query.strip()))

        # facts
        facts = []
        if time_elapsed:
            facts.append(("Time Elapsed", time_elapsed))
        if last_record_at:
            facts.append(("Last Record At", last_record_at))
        if sampled_at:
            facts.append(("Sampled At", sampled_at))

        if facts:
            result_blocks.append(FactsBlock(facts=facts))

        return result_blocks

    def _get_test_alert_config_blocks(
        self, test_params: Optional[Dict[str, Any]], fields: List[str]
    ) -> List[MessageBlock]:
        config_blocks: List[MessageBlock] = []
        if test_params and AlertField.TEST_PARAMS in fields:
            config_blocks.append(
                LinesBlock(
                    lines=[
                        BoldTextLineBlock(
                            text=[Icon.HAMMER_AND_WRENCH, "Test Parameters"]
                        ),
                    ]
                )
            )
            config_blocks.append(
                JsonCodeBlock(content=test_params),
            )
        return config_blocks

    def _get_model_alert_config_blocks(
        self,
        materialization: Optional[str] = None,
        full_refresh: Optional[bool] = None,
    ) -> List[MessageBlock]:
        facts = []
        if materialization:
            facts.append(("Materialization", materialization))
        if full_refresh is not None:
            facts.append(("Full Refresh", "Yes" if full_refresh else "No"))
        return [FactsBlock(facts=facts)]

    def _get_source_freshness_alert_config_blocks(
        self,
        error_after: Optional[str] = None,
        warn_after: Optional[str] = None,
        filter: Optional[str] = None,
    ) -> List[MessageBlock]:
        facts = []
        if error_after:
            facts.append(("Error after", error_after))
        if warn_after:
            facts.append(("Warn after", warn_after))
        if filter:
            facts.append(("Filter", filter))
        return [FactsBlock(facts=facts)] if facts else []

    def _get_alert_list_line(
        self,
        alert: Union[
            TestAlertModel,
            ModelAlertModel,
            SourceFreshnessAlertModel,
        ],
    ) -> LineBlock:
        inlines: List[InlineBlock] = [
            TextBlock(text=alert.summary, style=TextStyle.BOLD),
        ]
        if owners := list(set(alert.owners)):
            inlines.append(TextBlock(text="-"))
            owners.sort()
            inlines.append(TextBlock(text="Owners:"))
            inlines.append(MentionLineBlock(*owners))

        if self.config.alert_groups_subscribers:
            if subscribers := list(set(alert.subscribers)):
                if owners:
                    inlines.append(TextBlock(text="|"))
                else:
                    inlines.append(TextBlock(text="-"))
                subscribers.sort()
                inlines.append(TextBlock(text="Subscribers:"))
                inlines.append(MentionLineBlock(*subscribers))

        if report_link := alert.get_report_link():
            inlines.append(TextBlock(text="-"))
            inlines.append(LinkBlock(text=report_link.text, url=report_link.url))

        return LineBlock(inlines=inlines)

    def _get_alert_list_blocks(
        self,
        title: str,
        bullet_icon: Icon,
        alerts: Sequence[
            Union[
                TestAlertModel,
                ModelAlertModel,
                SourceFreshnessAlertModel,
            ]
        ],
    ) -> List[MessageBlock]:
        blocks: List[MessageBlock] = []
        if not alerts:
            return blocks
        blocks.append(LinesBlock(lines=[BoldTextLineBlock(text=title)]))
        lines = [self._get_alert_list_line(alert) for alert in alerts]
        bullet_list = BulletListBlock(icon=bullet_icon, lines=lines)
        blocks.append(bullet_list)
        return blocks

    def _get_sub_alert_groups_blocks(
        self,
        test_errors: List[Union[TestAlertModel, SourceFreshnessAlertModel]],
        test_warnings: List[Union[TestAlertModel, SourceFreshnessAlertModel]],
        test_failures: List[Union[TestAlertModel, SourceFreshnessAlertModel]],
        model_errors: List[ModelAlertModel],
    ) -> List[MessageBlock]:
        blocks: List[MessageBlock] = []
        model_errors_alert_list_blocks = self._get_alert_list_blocks(
            title="Model Errors",
            bullet_icon=Icon.X,
            alerts=model_errors,
        )

        test_failures_alert_list_blocks = self._get_alert_list_blocks(
            title="Test Failures",
            bullet_icon=Icon.RED_TRIANGLE,
            alerts=test_failures,
        )

        test_warnings_alert_list_blocks = self._get_alert_list_blocks(
            title="Test Warnings",
            bullet_icon=Icon.WARNING,
            alerts=test_warnings,
        )

        test_errors_alert_list_blocks = self._get_alert_list_blocks(
            title="Test Errors",
            bullet_icon=Icon.EXCLAMATION,
            alerts=test_errors,
        )

        blocks.extend(model_errors_alert_list_blocks)
        if blocks:
            blocks.append(DividerBlock())
        blocks.extend(test_failures_alert_list_blocks)
        if blocks and not isinstance(blocks[-1], DividerBlock):
            blocks.append(DividerBlock())
        blocks.extend(test_warnings_alert_list_blocks)
        if blocks and not isinstance(blocks[-1], DividerBlock):
            blocks.append(DividerBlock())
        blocks.extend(test_errors_alert_list_blocks)

        return blocks

    def _get_alert_title_blocks(
        self,
        alert: AlertType,
    ) -> List[MessageBlock]:
        test_type = alert.test_type if isinstance(alert, TestAlertModel) else None
        title = self._get_alert_title(alert.summary, alert.status, test_type)
        return [HeaderBlock(text=title)]

    def _get_alert_subtitle_blocks(
        self,
        alert: AlertType,
    ) -> List[MessageBlock]:
        if isinstance(
            alert, (TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel)
        ):
            return self._get_run_alert_subtitle_blocks(alert)
        elif isinstance(alert, AlertsGroup):
            return self._get_alerts_group_subtitle_blocks(alert)
        else:
            raise ValueError(f"Unknown alert type: {type(alert)}")

    def _get_alert_details_blocks(
        self,
        alert: AlertType,
        fields: List[str],
    ) -> List[MessageBlock]:
        if isinstance(alert, TestAlertModel):
            return self._get_details_blocks(
                table=alert.table_full_name,
                column=alert.column_name,
                tags=alert.tags,
                owners=alert.owners,
                subscribers=alert.subscribers,
                description=alert.test_description,
                fields=fields,
            )
        elif isinstance(alert, ModelAlertModel):
            return self._get_details_blocks(
                tags=alert.tags,
                owners=alert.owners,
                subscribers=alert.subscribers,
                path=alert.original_path,
                fields=fields,
            )
        elif isinstance(alert, SourceFreshnessAlertModel):
            return self._get_details_blocks(
                tags=alert.tags,
                owners=alert.owners,
                subscribers=alert.subscribers,
                path=alert.path,
                description=alert.freshness_description,
                fields=fields,
            )
        elif isinstance(alert, GroupedByTableAlerts):
            return self._get_details_blocks(
                tags=alert.tags,
                owners=alert.owners,
                subscribers=alert.subscribers,
                fields=fields,
            )
        return []

    def _get_alert_result_blocks(
        self,
        alert: AlertType,
        fields: List[str],
    ) -> List[MessageBlock]:
        result_blocks: List[MessageBlock] = []
        title = "Result"

        if isinstance(alert, TestAlertModel):
            is_anomaly_detection = alert.test_type == "anomaly_detection"
            result_blocks = self._get_result_blocks(
                result_message=alert.error_message,
                result_sample=(
                    alert.test_rows_sample if not is_anomaly_detection else None
                ),
                anomalous_value=(alert.other if is_anomaly_detection else None),
                result_query=alert.test_results_query,
                fields=fields,
            )
            title = "Test Result"
        elif isinstance(alert, ModelAlertModel):
            if alert.message:
                result_blocks = self._get_result_blocks(
                    result_message=alert.message,
                    fields=fields,
                )
        elif isinstance(alert, SourceFreshnessAlertModel):
            result_blocks = self._get_result_blocks(
                result_message=alert.error_message,
                time_elapsed=f"{timedelta(seconds=alert.max_loaded_at_time_ago_in_s) if alert.max_loaded_at_time_ago_in_s else 'N/A'}",
                last_record_at=alert.max_loaded_at,
                sampled_at=alert.snapshotted_at_str,
                fields=fields,
            )

        if result_blocks:
            return [ExpandableBlock(title=title, body=result_blocks)]
        return []

    def _get_alert_config_blocks(
        self,
        alert: AlertType,
        fields: List[str],
    ) -> List[MessageBlock]:
        config_blocks: List[MessageBlock] = []
        title = "Configuration"
        expandable = False

        if isinstance(alert, TestAlertModel):
            config_blocks = self._get_test_alert_config_blocks(
                alert.test_params, fields
            )
            title = "Test Configuration"
        elif isinstance(alert, ModelAlertModel):
            if alert.materialization != "snapshot":
                config_blocks = self._get_model_alert_config_blocks(
                    materialization=alert.materialization,
                    full_refresh=alert.full_refresh,
                )
                title = "Model Configuration"
                expandable = True
        elif isinstance(alert, SourceFreshnessAlertModel):
            config_blocks = self._get_source_freshness_alert_config_blocks(
                error_after=alert.error_after,
                warn_after=alert.warn_after,
                filter=alert.filter,
            )
            title = "Source Freshness Configuration"

        if config_blocks:
            return [
                ExpandableBlock(title=title, body=config_blocks, expanded=expandable)
            ]
        return []

    def _get_alert_groups_blocks(
        self,
        alert: BaseAlertsGroup,
    ) -> List[MessageBlock]:
        if isinstance(alert, AlertsGroup):
            return self._get_sub_alert_groups_blocks(
                model_errors=alert.model_errors,
                test_failures=alert.test_failures,
                test_warnings=alert.test_warnings,
                test_errors=alert.test_errors,
            )
        else:
            raise ValueError(f"Unknown alert type: {type(alert)}")

    def build(
        self,
        alert: AlertType,
    ) -> MessageBody:
        color = self._get_alert_color(alert)

        fields = alert.alert_fields if not isinstance(alert, BaseAlertsGroup) else None
        fields = fields or [field.value for field in AlertField]

        blocks: List[MessageBlock] = []

        title_blocks = self._get_alert_title_blocks(alert)
        blocks.extend(title_blocks)

        subtitle_blocks = self._get_alert_subtitle_blocks(alert)
        blocks.extend(subtitle_blocks)

        blocks.append(DividerBlock())

        details_blocks = self._get_alert_details_blocks(alert, fields)
        if details_blocks:
            blocks.extend(details_blocks)
            blocks.append(DividerBlock())

        result_blocks = self._get_alert_result_blocks(alert, fields)
        blocks.extend(result_blocks)

        config_blocks = self._get_alert_config_blocks(alert, fields)
        blocks.extend(config_blocks)

        if isinstance(alert, BaseAlertsGroup):
            alert_groups_blocks = self._get_alert_groups_blocks(alert)
            blocks.extend(alert_groups_blocks)

        if isinstance(blocks[-1], DividerBlock):
            blocks.pop()

        message_body = MessageBody(
            color=color,
            blocks=blocks,
        )
        return message_body
