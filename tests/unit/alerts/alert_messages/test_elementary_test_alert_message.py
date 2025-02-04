import itertools
from pathlib import Path

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from tests.unit.alerts.alert_messages.test_alert_utils import (
    BOOLEAN_VALUES,
    STATUS_VALUES,
    build_base_test_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def build_elementary_test_alert_model(*args, **kwargs):
    kwargs.pop("test_type", None)  # Remove test_type if present
    test_type = (
        "anomaly_detection" if kwargs.pop("has_anomaly", False) else "schema_change"
    )
    return build_base_test_alert_model(*args, test_type=test_type, **kwargs)


# Generate all combinations of test parameters
params = [
    STATUS_VALUES,  # status
    BOOLEAN_VALUES,  # has_link
    BOOLEAN_VALUES,  # has_description
    BOOLEAN_VALUES,  # has_tags
    BOOLEAN_VALUES,  # has_owners
    BOOLEAN_VALUES,  # has_table
    BOOLEAN_VALUES,  # has_error
    BOOLEAN_VALUES,  # has_sample
    BOOLEAN_VALUES,  # has_anomaly
]
combinations = list(itertools.product(*params))


@pytest.mark.parametrize(
    "status,has_link,has_description,has_tags,has_owners,has_table,has_error,has_sample,has_anomaly",
    combinations,
)
def test_get_elementary_test_alert_message_body(
    monkeypatch,
    status: str,
    has_link: bool,
    has_description: bool,
    has_tags: bool,
    has_owners: bool,
    has_table: bool,
    has_error: bool,
    has_sample: bool,
    has_anomaly: bool,
):
    test_alert_model = build_elementary_test_alert_model(
        status=status,
        table_name=None if not has_table else "test_table",
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        test_description="Test description" if has_description else None,
        error_message="Test error message" if has_error else None,
        test_rows_sample=(
            {"column1": "value1", "column2": "value2"} if has_sample else None
        ),
        test_results_query="SELECT * FROM test" if has_sample else None,
        test_params={"param1": "value1"} if has_sample else None,
        has_anomaly=has_anomaly,
        other={"anomalous_value": 42} if has_anomaly else None,
    )

    monkeypatch.setattr(
        test_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_alert_message_body(test_alert_model)
    adaptive_card_filename = f"adaptive_card_elementary_test_alert_status-{status}_link-{has_link}_description-{has_description}_tags-{has_tags}_owners-{has_owners}_table-{has_table}_error-{has_error}_sample-{has_sample}_anomaly-{has_anomaly}.json"
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
