from datetime import timedelta
from typing import Any, Dict, List, Optional, Sequence, Union

from elementary.messages.block_builders import (
    BoldTextLineBlock,
    BulletListBlock,
    FactsBlock,
    JsonCodeBlock,
    LinkLineBlock,
    SummaryLineBlock,
)
from elementary.messages.blocks import (
    CodeBlock,
    DividerBlock,
    ExpandableBlock,
    HeaderBlock,
    Icon,
    InlineBlock,
    LineBlock,
    LinesBlock,
    LinkBlock,
    TextBlock,
    TextStyle,
)
from elementary.messages.message_body import Color, MessageBlock, MessageBody
from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.alerts_groups.grouped_by_table import (
    GroupedByTableAlerts,
)
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
)

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


def get_display_name(status: Optional[str]) -> Optional[str]:
    if status is None:
        return None
    return STATUS_DISPLAYS.get(status, status.capitalize())


def get_color(alert_status: Optional[str]) -> Optional[Color]:
    if alert_status is None:
        return None
    return STATUS_COLORS.get(alert_status)


def get_test_alert_title(
    summary: str, status: Optional[str], test_type: Optional[str]
) -> str:
    if test_type == "schema_change":
        return summary
    return f"{get_display_name(status)}: {summary}" if status else summary


def get_run_alert_subtitle_block(
    test: Optional[str] = None,
    snapshot: Optional[str] = None,
    model: Optional[str] = None,
    source: Optional[str] = None,
    status: Optional[str] = None,
    detected_at_str: Optional[str] = None,
    suppression_interval: Optional[int] = None,
    report_link: Optional[ReportLinkData] = None,
) -> LinesBlock:
    summary = []
    if test:
        summary.append(("Test:", test))
    if snapshot:
        summary.append(("Snapshot:", snapshot))
    if model:
        summary.append(("Model:", model))
    if source:
        summary.append(("Source:", source))
    summary.append(("Status:", status or "Unknown"))
    if detected_at_str:
        summary.append(("Time:", detected_at_str))
    if suppression_interval:
        summary.append(("Suppression interval:", str(suppression_interval)))
    subtitle_lines = [SummaryLineBlock(summary=summary)]

    if report_link:
        subtitle_lines.append(
            LinkLineBlock(text="View in Elementary", url=report_link.url)
        )
    return LinesBlock(lines=subtitle_lines)


def get_alert_type_counters_subtitle_block(
    model_errors_count: int = 0,
    test_failures_count: int = 0,
    test_warnings_count: int = 0,
    test_errors_count: int = 0,
) -> LinesBlock:
    summary = []
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
    subtitle_lines = [SummaryLineBlock(summary=summary)]

    return LinesBlock(lines=subtitle_lines)


def get_details_blocks(
    table: Optional[str] = None,
    column: Optional[str] = None,
    tags: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    subscribers: Optional[List[str]] = None,
    description: Optional[str] = None,
    path: Optional[str] = None,
) -> List[MessageBlock]:
    blocks: List[MessageBlock] = []
    if not (table or column or tags or owners or subscribers or description or path):
        return blocks
    blocks.append(
        LinesBlock(
            lines=[
                BoldTextLineBlock(text=[Icon.INFO, "Details"]),
            ]
        )
    )
    facts = []
    if table:
        facts.append(("Table", table))
    if column:
        facts.append(("Column", column))

    facts.append(("Tags", ", ".join(tags) if tags else "No tags"))
    facts.append(("Owners", ", ".join(owners) if owners else "No owners"))
    facts.append(
        ("Subscribers", ", ".join(subscribers) if subscribers else "No subscribers")
    )

    if description:
        facts.append(("Description", description))
    if path:
        facts.append(("Path", path))
    blocks.append(FactsBlock(facts=facts))
    return blocks


def get_result_blocks(
    result_message: Optional[str],
    result_sample: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]] = None,
    result_query: Optional[str] = None,
    anomalous_value: Optional[dict] = None,
    time_elapsed: Optional[str] = None,
    last_record_at: Optional[str] = None,
    sampled_at: Optional[str] = None,
) -> List[MessageBlock]:
    result_blocks: List[MessageBlock] = []
    if result_message:
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
    if result_sample:
        result_blocks.append(
            LinesBlock(
                lines=[
                    BoldTextLineBlock(
                        text=[Icon.MAGNIFYING_GLASS, "Test Results Sample"]
                    ),
                ]
            )
        )
        result_blocks.append(
            JsonCodeBlock(content=result_sample),
        )
    if result_query:
        result_blocks.append(
            LinesBlock(
                lines=[
                    BoldTextLineBlock(text=["Test Results Query"]),
                ]
            )
        )
        result_blocks.append(CodeBlock(text=result_query.strip()))
    if anomalous_value:
        result_blocks.append(
            LinesBlock(
                lines=[
                    BoldTextLineBlock(text=["Anomalous Values"]),
                ]
            )
        )
        result_blocks.append(
            JsonCodeBlock(content=anomalous_value),
        )

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


def get_test_alert_config_blocks(
    test_params: Optional[Dict[str, Any]]
) -> List[MessageBlock]:
    config_blocks: List[MessageBlock] = []
    if test_params:
        config_blocks.append(
            LinesBlock(
                lines=[
                    BoldTextLineBlock(text=[Icon.HAMMER_AND_WRENCH, "Test Parameters"]),
                ]
            )
        )
        config_blocks.append(
            JsonCodeBlock(content=test_params),
        )
    return config_blocks


def get_model_alert_config_blocks(
    materialization: Optional[str] = None,
    full_refresh: Optional[bool] = None,
) -> List[MessageBlock]:
    facts = []
    if materialization:
        facts.append(("Materialization", materialization))
    if full_refresh:
        facts.append(("Full Refresh", "Yes" if full_refresh else "No"))
    return [FactsBlock(facts=facts)]


def get_source_freshness_alert_config_blocks(
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


def get_alert_list_line(
    alert: Union[
        TestAlertModel,
        ModelAlertModel,
        SourceFreshnessAlertModel,
    ]
) -> LineBlock:
    inlines: List[InlineBlock] = [
        TextBlock(text=alert.summary, style=TextStyle.BOLD),
    ]
    if owners := list(set(alert.owners)):
        inlines.append(TextBlock(text="-"))
        if len(owners) == 1:
            inlines.append(TextBlock(text=f"Owner: {owners.pop()}"))
        else:
            # order owners by alphabetical order
            owners.sort()
            inlines.append(TextBlock(text=f"Owners: {', '.join(owners)}"))

    if report_link := alert.get_report_link():
        inlines.append(TextBlock(text="-"))
        inlines.append(LinkBlock(text=report_link.text, url=report_link.url))

    return LineBlock(inlines=inlines)


def get_alert_list_blocks(
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
    lines = [get_alert_list_line(alert) for alert in alerts]
    bullet_list = BulletListBlock(icon=bullet_icon, lines=lines)
    blocks.append(bullet_list)
    return blocks


def get_dbt_test_alert_message_body(alert: TestAlertModel) -> MessageBody:
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    title = get_test_alert_title(alert.summary, alert.status, alert.test_type)
    title_block = HeaderBlock(text=title)
    blocks.append(title_block)
    subtitle_block = get_run_alert_subtitle_block(
        test=alert.concise_name,
        status=alert.status,
        detected_at_str=alert.detected_at_str,
        report_link=alert.get_report_link(),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    details_blocks = get_details_blocks(
        table=alert.table_full_name,
        column=alert.column_name,
        tags=alert.tags,
        owners=alert.owners,
        subscribers=alert.subscribers,
        description=alert.test_description,
    )
    if details_blocks:
        blocks.extend(details_blocks)
        blocks.append(DividerBlock())

    result_blocks = get_result_blocks(
        result_message=alert.error_message,
        result_sample=alert.test_rows_sample,
    )
    if result_blocks:
        blocks.append(ExpandableBlock(title="Test Result", body=result_blocks))

    config_blocks = get_test_alert_config_blocks(alert.test_params)
    if config_blocks:
        blocks.append(ExpandableBlock(title="Test Configuration", body=config_blocks))

    if isinstance(blocks[-1], DividerBlock):
        blocks.pop()

    message_body = MessageBody(
        color=color,
        blocks=blocks,
    )
    return message_body


def get_elementary_test_alert_message_body(alert: TestAlertModel) -> MessageBody:
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    anomalous_value = alert.other if alert.test_type == "anomaly_detection" else None
    title = get_test_alert_title(alert.summary, alert.status, alert.test_type)
    blocks.append(HeaderBlock(text=title))
    subtitle_block = get_run_alert_subtitle_block(
        test=alert.concise_name,
        status=alert.status,
        detected_at_str=alert.detected_at_str,
        suppression_interval=alert.suppression_interval,
        report_link=alert.get_report_link(),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    details_blocks = get_details_blocks(
        table=alert.table_full_name,
        column=alert.column_name,
        tags=alert.tags,
        owners=alert.owners,
        subscribers=alert.subscribers,
        description=alert.test_description,
    )
    if details_blocks:
        blocks.extend(details_blocks)
        blocks.append(DividerBlock())

    result_blocks = get_result_blocks(
        result_message=alert.error_message,
        result_sample=alert.test_rows_sample,
        anomalous_value=(
            anomalous_value if alert.test_type == "anomaly_detection" else None
        ),
        result_query=alert.test_results_query,
    )

    if result_blocks:
        blocks.append(ExpandableBlock(title="Test Result", body=result_blocks))

    config_blocks = get_test_alert_config_blocks(alert.test_params)
    if config_blocks:
        blocks.append(ExpandableBlock(title="Test Configuration", body=config_blocks))

    if isinstance(blocks[-1], DividerBlock):
        blocks.pop()

    message_body = MessageBody(
        color=color,
        blocks=blocks,
    )
    return message_body


def get_snapshot_alert_message_body(alert: ModelAlertModel) -> MessageBody:
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    # Title using helper function
    title = get_test_alert_title(alert.summary, alert.status, None)
    blocks.append(HeaderBlock(text=title))

    # Subtitle using helper function
    subtitle_block = get_run_alert_subtitle_block(
        snapshot=alert.alias,
        status=alert.status,
        detected_at_str=alert.detected_at_str,
        suppression_interval=alert.suppression_interval,
        report_link=alert.get_report_link(),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    # Details section
    details_blocks = get_details_blocks(
        tags=alert.tags,
        owners=alert.owners,
        subscribers=alert.subscribers,
        path=alert.original_path,
    )
    if details_blocks:
        blocks.extend(details_blocks)
        blocks.append(DividerBlock())

    # Result section
    result_blocks = get_result_blocks(
        result_message=alert.message,
    )
    if result_blocks:
        blocks.append(ExpandableBlock(title="Result", body=result_blocks))

    if isinstance(blocks[-1], DividerBlock):
        blocks.pop()

    message_body = MessageBody(
        color=color,
        blocks=blocks,
    )
    return message_body


def get_model_alert_message_body(alert: ModelAlertModel) -> MessageBody:
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    # Title using helper function
    title = get_test_alert_title(alert.summary, alert.status, None)
    blocks.append(HeaderBlock(text=title))

    # Subtitle using helper function
    subtitle_block = get_run_alert_subtitle_block(
        model=alert.alias,
        status=alert.status,
        detected_at_str=alert.detected_at_str,
        suppression_interval=alert.suppression_interval,
        report_link=alert.get_report_link(),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    details_blocks = get_details_blocks(
        tags=alert.tags,
        owners=alert.owners,
        subscribers=alert.subscribers,
        path=alert.original_path,
    )
    if details_blocks:
        blocks.extend(details_blocks)
        blocks.append(DividerBlock())

    config_blocks = get_model_alert_config_blocks(
        materialization=alert.materialization,
        full_refresh=alert.full_refresh,
    )
    if config_blocks:
        blocks.append(
            ExpandableBlock(
                title="Model Configuration",
                body=config_blocks,
                expanded=True,
            )
        )

    if isinstance(blocks[-1], DividerBlock):
        blocks.pop()

    message_body = MessageBody(
        color=color,
        blocks=blocks,
    )
    return message_body


def get_source_freshness_alert_message_body(alert: SourceFreshnessAlertModel) -> MessageBody:  # type: ignore
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    title = get_test_alert_title(alert.summary, alert.status, None)
    blocks.append(HeaderBlock(text=title))

    subtitle_block = get_run_alert_subtitle_block(
        source=f"{alert.source_name}.{alert.identifier}",
        status=alert.status,
        detected_at_str=alert.detected_at_str,
        suppression_interval=alert.suppression_interval,
        report_link=alert.get_report_link(),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    details_blocks = get_details_blocks(
        tags=alert.tags,
        owners=alert.owners,
        subscribers=alert.subscribers,
        path=alert.path,
        description=alert.freshness_description,
    )
    if details_blocks:
        blocks.extend(details_blocks)
        blocks.append(DividerBlock())

    message = (
        (f"Failed to calculate the source freshness\n" f"```{alert.error}```")
        if alert.status == "runtime error"
        else alert.result_description
    )

    result_blocks = get_result_blocks(
        result_message=message,
        time_elapsed=f"{timedelta(seconds=alert.max_loaded_at_time_ago_in_s) if alert.max_loaded_at_time_ago_in_s else 'N/A'}",
        last_record_at=alert.max_loaded_at,
        sampled_at=alert.snapshotted_at_str,
    )
    if result_blocks:
        blocks.append(ExpandableBlock(title="Result", body=result_blocks))

    config_blocks = get_source_freshness_alert_config_blocks(
        error_after=alert.error_after,
        warn_after=alert.warn_after,
        filter=alert.filter,
    )
    if config_blocks:
        blocks.append(
            ExpandableBlock(
                title="Source Freshness Configuration",
                body=config_blocks,
            )
        )

    if isinstance(blocks[-1], DividerBlock):
        blocks.pop()

    message_body = MessageBody(
        color=color,
        blocks=blocks,
    )
    return message_body


def get_alerts_group_message_body(alert: AlertsGroup) -> MessageBody:
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    title = get_test_alert_title(alert.summary, alert.status, None)
    blocks.append(HeaderBlock(text=title))

    subtitle_block = get_alert_type_counters_subtitle_block(
        model_errors_count=len(alert.model_errors),
        test_failures_count=len(alert.test_failures),
        test_warnings_count=len(alert.test_warnings),
        test_errors_count=len(alert.test_errors),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    model_errors_alert_list_blocks = get_alert_list_blocks(
        title="Model Errors",
        bullet_icon=Icon.X,
        alerts=alert.model_errors,
    )
    blocks.extend(model_errors_alert_list_blocks)

    test_failures_alert_list_blocks = get_alert_list_blocks(
        title="Test Failures",
        bullet_icon=Icon.RED_TRIANGLE,
        alerts=alert.test_failures,
    )
    blocks.extend(test_failures_alert_list_blocks)

    test_warnings_alert_list_blocks = get_alert_list_blocks(
        title="Test Warnings",
        bullet_icon=Icon.WARNING,
        alerts=alert.test_warnings,
    )
    blocks.extend(test_warnings_alert_list_blocks)

    test_errors_alert_list_blocks = get_alert_list_blocks(
        title="Test Errors",
        bullet_icon=Icon.EXCLAMATION,
        alerts=alert.test_errors,
    )
    blocks.extend(test_errors_alert_list_blocks)

    if isinstance(blocks[-1], DividerBlock):
        blocks.pop()

    message_body = MessageBody(
        color=color,
        blocks=blocks,
    )
    return message_body


def get_group_by_table_alert_message_body(alert: GroupedByTableAlerts) -> MessageBody:
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    title = get_test_alert_title(alert.summary, alert.status, None)
    blocks.append(HeaderBlock(text=title))

    subtitle_block = get_alert_type_counters_subtitle_block(
        model_errors_count=len(alert.model_errors),
        test_failures_count=len(alert.test_failures),
        test_warnings_count=len(alert.test_warnings),
        test_errors_count=len(alert.test_errors),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    details_blocks = get_details_blocks(
        tags=alert.tags,
        owners=alert.owners,
        subscribers=alert.subscribers,
    )
    if details_blocks:
        blocks.extend(details_blocks)
        blocks.append(DividerBlock())

    model_errors_alert_list_blocks = get_alert_list_blocks(
        title="Model Errors",
        bullet_icon=Icon.X,
        alerts=alert.model_errors,
    )
    blocks.extend(model_errors_alert_list_blocks)

    test_failures_alert_list_blocks = get_alert_list_blocks(
        title="Test Failures",
        bullet_icon=Icon.RED_TRIANGLE,
        alerts=alert.test_failures,
    )
    blocks.extend(test_failures_alert_list_blocks)

    test_warnings_alert_list_blocks = get_alert_list_blocks(
        title="Test Warnings",
        bullet_icon=Icon.WARNING,
        alerts=alert.test_warnings,
    )
    blocks.extend(test_warnings_alert_list_blocks)

    test_errors_alert_list_blocks = get_alert_list_blocks(
        title="Test Errors",
        bullet_icon=Icon.EXCLAMATION,
        alerts=alert.test_errors,
    )
    blocks.extend(test_errors_alert_list_blocks)

    if isinstance(blocks[-1], DividerBlock):
        blocks.pop()
    message_body = MessageBody(
        color=color,
        blocks=blocks,
    )
    return message_body
