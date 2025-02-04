from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from tests.unit.alerts.alert_messages.test_alert_utils import (
    create_test_alerts,
    get_alert_message_body,
    get_alerts_group_test_params,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.mark.parametrize(
    "has_model_errors,has_test_failures,has_test_warnings,has_test_errors,has_link",
    get_alerts_group_test_params(),
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

    message_body = get_alert_message_body(alerts_group)
    adaptive_card_filename = f"adaptive_card_alerts_group_model-errors-{has_model_errors}_test-failures-{has_test_failures}_test-warnings-{has_test_warnings}_test-errors-{has_test_errors}_link-{has_link}.json"
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)

    # Stop all patches
    for patcher in patches:
        patcher.stop()
