from typing import List

from elementary.messages.blocks import ExpandableBlock, LinesBlock, TableBlock
from elementary.monitor.alerts.alert_messages.builder import MessageBuilderConfig
from tests.unit.alerts.alert_messages.test_alert_utils import (
    build_base_test_alert_model,
    get_alert_message_body,
)


def _build_test_rows_sample(row_count: int) -> List[dict]:
    return [{"column1": f"value_{i}", "column2": i} for i in range(row_count)]


def _get_test_result_expandable(message_body) -> ExpandableBlock:
    for block in message_body.blocks:
        if isinstance(block, ExpandableBlock) and block.title == "Test Result":
            return block
    raise AssertionError("Expected a 'Test Result' expandable block")


def _get_table_block(blocks) -> TableBlock:
    for block in blocks:
        if isinstance(block, TableBlock):
            return block
    raise AssertionError("Expected a table block among the result blocks")


def _extract_line_texts(blocks) -> List[str]:
    texts: List[str] = []
    for block in blocks:
        if not isinstance(block, LinesBlock):
            continue
        for line in block.lines:
            for inline in line.inlines:
                text = getattr(inline, "text", None)
                if text:
                    texts.append(text)
    return texts


def test_large_result_sample_is_truncated_to_configured_row_limit():
    config = MessageBuilderConfig(maximum_rows_in_alert_samples=10)
    alert = build_base_test_alert_model(
        status="fail",
        table_name="test_table",
        tags=None,
        owners=None,
        subscribers=None,
        test_description=None,
        error_message=None,
        test_rows_sample=_build_test_rows_sample(37),
        test_results_query=None,
        test_params=None,
    )

    message_body = get_alert_message_body(alert, config=config)
    result_body = _get_test_result_expandable(message_body).body

    table_block = _get_table_block(result_body)
    assert len(table_block.rows) == 10

    truncation_notes = _extract_line_texts(result_body)
    assert any(
        "Showing 10 of 37 rows" in text and "27 omitted" in text
        for text in truncation_notes
    ), f"Expected a truncation note, got: {truncation_notes}"


def test_small_result_sample_is_not_truncated():
    config = MessageBuilderConfig(maximum_rows_in_alert_samples=10)
    alert = build_base_test_alert_model(
        status="fail",
        table_name="test_table",
        tags=None,
        owners=None,
        subscribers=None,
        test_description=None,
        error_message=None,
        test_rows_sample=_build_test_rows_sample(3),
        test_results_query=None,
        test_params=None,
    )

    message_body = get_alert_message_body(alert, config=config)
    result_body = _get_test_result_expandable(message_body).body

    table_block = _get_table_block(result_body)
    assert len(table_block.rows) == 3

    truncation_notes = _extract_line_texts(result_body)
    assert not any("omitted" in text for text in truncation_notes)
