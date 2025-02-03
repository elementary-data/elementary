from typing import Any, Dict, List, Optional, Union

from elementary.messages.block_builders import (
    BoldTextLineBlock,
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
    LinesBlock,
)
from elementary.messages.message_body import Color, MessageBlock, MessageBody
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.utils.report_link import (
    ReportLinkData,
)

STATUS_DISPLAYS: Dict[str, str] = {
    "fail": "Failure",
    "warn": "Warning",
    "error": "Error",
}

STATUS_COLORS: Dict[str, Color] = {
    "fail": Color.RED,
    "warn": Color.YELLOW,
    "error": Color.RED,
}


def get_display_name(alert_status: Optional[str]) -> str:
    if alert_status is None:
        return "Unknown"
    return STATUS_DISPLAYS.get(alert_status, alert_status.capitalize())


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


def get_test_alert_title_block(summary: str, status: Optional[str]) -> HeaderBlock:
    title = f"{get_display_name(status)}: {summary}" if status else summary
    return HeaderBlock(text=title)


def get_test_alert_subtitle_block(
    test: Optional[str] = None,
    snapshot: Optional[str] = None,
    model: Optional[str] = None,
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
    if tags:
        facts.append(("Tags", ", ".join(tags or [])))
    if owners:
        facts.append(("Owners", ", ".join(owners or [])))
    if subscribers:
        facts.append(("Subscribers", ", ".join(subscribers or [])))
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


def get_dbt_test_alert_message_body(alert: TestAlertModel) -> MessageBody:
    color = get_color(alert.status)
    blocks: List[MessageBlock] = []

    title = get_test_alert_title(alert.summary, alert.status, alert.test_type)
    title_block = HeaderBlock(text=title)
    blocks.append(title_block)
    subtitle_block = get_test_alert_subtitle_block(
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
    subtitle_block = get_test_alert_subtitle_block(
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
    subtitle_block = get_test_alert_subtitle_block(
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

    return MessageBody(
        color=color,
        blocks=blocks,
    )
