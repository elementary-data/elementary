import itertools
from datetime import datetime
from pathlib import Path
from typing import List, Union
from unittest.mock import patch

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.monitor.alerts.alert_messages.test_alert_message import (
    get_alerts_group_message_body,
)
from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from tests.unit.alerts.alert_messages.test_alert_utils import (
    build_base_model_alert_model,
    build_base_test_alert_model,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def create_test_alerts(
    has_model_errors: bool,
    has_test_failures: bool,
    has_test_warnings: bool,
    has_test_errors: bool,
    detected_at: datetime,
    count: int,
    has_link: bool,
) -> List[Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]]:
    alerts: List[Union[TestAlertModel, ModelAlertModel, SourceFreshnessAlertModel]] = []

    def _get_owners_by_mod(i: int):
        mod_value = i % 3
        if mod_value == 0:
            return []
        elif mod_value == 1:
            return ["owner1"]
        else:  # mod_value == 2
            return ["owner1", "owner2"]

    if has_model_errors:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            model_alert = build_base_model_alert_model(
                status="error",
                tags=["tag1"],
                owners=owners,
                path="models/test_model.sql",
                materialization="table",
                full_refresh=True,
                detected_at=detected_at,
                alias="test_model",
                message="Test model error",
            )
            alerts.append(model_alert)

    if has_test_failures:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            test_alert = build_base_test_alert_model(
                status="fail",
                table_name=f"test_table_{i + 1}",
                tags=["tag1"],
                owners=owners,
                test_description="Test failure description",
                error_message="Test failure message",
                test_rows_sample=None,
                test_results_query=None,
                test_params=None,
            )
            alerts.append(test_alert)

    if has_test_warnings:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            test_alert = build_base_test_alert_model(
                status="warn",
                table_name=f"test_table_{i + 1}",
                tags=["tag1"],
                owners=owners,
                test_description=f"Test warning description {i + 1}",
                error_message=f"Test warning message {i + 1}",
                test_rows_sample=None,
                test_results_query=None,
                test_params=None,
            )
            alerts.append(test_alert)

    if has_test_errors:
        for i in range(count):
            owners = _get_owners_by_mod(i)
            test_alert = build_base_test_alert_model(
                status="error",
                table_name=f"test_table_{i + 1}",
                tags=["tag1"],
                owners=owners,
                test_description=f"Test error description {i + 1}",
                error_message="Test error message",
                test_rows_sample=None,
                test_results_query=None,
                test_params=None,
            )
            alerts.append(test_alert)

    return alerts


# Generate all combinations of test parameters
# Each boolean represents whether that type of alert exists in the group
# We ensure at least one type exists by excluding the all-False case
params = [
    (has_model_errors, has_test_failures, has_test_warnings, has_test_errors, has_link)
    for has_model_errors, has_test_failures, has_test_warnings, has_test_errors, has_link in itertools.product(
        [True, False], repeat=5
    )
    if any([has_model_errors, has_test_failures, has_test_warnings, has_test_errors])
]


@pytest.mark.parametrize(
    "has_model_errors,has_test_failures,has_test_warnings,has_test_errors,has_link",
    params,
)
def test_get_alerts_group_message_body(
    has_model_errors: bool,
    has_test_failures: bool,
    has_test_warnings: bool,
    has_test_errors: bool,
    has_link: bool,
):
    detected_at = datetime(2025, 2, 3, 13, 21, 7)

    alerts = create_test_alerts(
        has_model_errors=has_model_errors,
        has_test_failures=has_test_failures,
        has_test_warnings=has_test_warnings,
        has_test_errors=has_test_errors,
        detected_at=detected_at,
        count=10,
        has_link=has_link,
    )

    # Create a list of patches for all alerts
    patches = [
        patch.object(alert, "get_report_link", lambda: get_mock_report_link(has_link))
        for alert in alerts
    ]

    # Apply all patches
    for patcher in patches:
        patcher.start()

    alerts_group = AlertsGroup(alerts=alerts)

    message_body = get_alerts_group_message_body(alerts_group)
    adaptive_card_filename = f"adaptive_card_alerts_group_model-errors-{has_model_errors}_test-failures-{has_test_failures}_test-warnings-{has_test_warnings}_test-errors-{has_test_errors}_link-{has_link}.json"
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)

    # Stop all patches
    for patcher in patches:
        patcher.stop()
