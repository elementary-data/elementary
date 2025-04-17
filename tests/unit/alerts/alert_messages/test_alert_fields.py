from pathlib import Path

import pytest

from elementary.monitor.alerts.alert_messages.alert_fields import AlertField
from tests.unit.alerts.alert_messages.test_alert_utils import (
    assert_expected_json_on_all_formats,
    build_base_test_alert_model,
    get_alert_message_body,
)

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
        subscribers=None,
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
    filename = f"dbt_test_alert_removed_field-{removed_field.value}"
    assert_expected_json_on_all_formats(filename, message_body)
