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


def get_test_alert_title_block(summary: str, status: Optional[str]) -> HeaderBlock:
    title = f"{get_display_name(status)}: {summary}" if status else summary
    return HeaderBlock(text=title)


def get_test_alert_subtitle_block(
    test: Optional[str] = None,
    status: Optional[str] = None,
    detected_at_str: Optional[str] = None,
    suppression_interval: Optional[int] = None,
    report_link: Optional[ReportLinkData] = None,
) -> LinesBlock:
    summary = []
    if test:
        summary.append(("Test:", test))
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


def get_test_alert_details_blocks(
    table: Optional[str] = None,
    column: Optional[str] = None,
    tags: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    subscribers: Optional[List[str]] = None,
    description: Optional[str] = None,
) -> List[MessageBlock]:
    blocks: List[MessageBlock] = []
    if not (table or tags or owners or subscribers or description):
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
    blocks.append(FactsBlock(facts=facts))
    return blocks


def get_test_alert_result_blocks(
    result_message: Optional[str],
    result_sample: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]],
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

    title_block = get_test_alert_title_block(alert.summary, alert.status)
    blocks.append(title_block)
    subtitle_block = get_test_alert_subtitle_block(
        test=alert.concise_name,
        status=alert.status,
        detected_at_str=alert.detected_at_str,
        report_link=alert.get_report_link(),
    )
    blocks.append(subtitle_block)
    blocks.append(DividerBlock())

    details_blocks = get_test_alert_details_blocks(
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

    result_blocks = get_test_alert_result_blocks(
        alert.error_message, alert.test_rows_sample
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
