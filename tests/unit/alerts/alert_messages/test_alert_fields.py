from pathlib import Path

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.monitor.alerts.alert_messages.alert_fields import AlertField
from tests.unit.alerts.alert_messages.test_alert_utils import (
    build_base_test_alert_model,
    get_alert_message_body,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def build_dbt_test_alert_model(*args, **kwargs):
    kwargs.pop("test_type", None)  # Remove test_type if present
    return build_base_test_alert_model(*args, test_type="dbt_test", **kwargs)


@pytest.mark.parametrize(
    "removed_field",
    list(AlertField),
)
def test_get_dbt_test_alert_message_body(
    monkeypatch,
    removed_field: AlertField,
):
    test_alert_model = build_dbt_test_alert_model(
        status="fail",
        table_name="test_table",
        column_name="test_column",
        tags=["tag1", "tag2"],
        owners=["owner1", "owner2"],
        test_description="Test description",
        error_message="Test error message",
        test_rows_sample=({"column1": "value1", "column2": "value2"}),
        test_results_query="select 1",
        test_params={"param1": "value1", "param2": "value2"},
    )
    test_alert_model.alert_fields = [
        field.value for field in AlertField if field != removed_field
    ]

    message_body = get_alert_message_body(test_alert_model)
    adaptive_card_filename = (
        f"adaptive_card_dbt_test_alert_removed_field-{removed_field.value}.json"
    )
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
