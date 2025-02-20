from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from tests.unit.alerts.alert_messages.test_alert_utils import (
    assert_expected_json_on_all_formats,
    create_test_alerts,
    get_alert_message_body,
    get_mock_report_link,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.mark.parametrize(
    "has_model_errors,has_test_failures,has_test_warnings,has_test_errors,has_link,has_env",
    [
        (True, True, True, True, True, True),
        (True, True, True, False, True, True),
        (True, True, False, True, True, True),
        (True, False, True, True, True, True),
        (False, True, True, True, True, True),
        (False, True, False, True, False, False),
        (True, False, True, False, True, False),
        (False, False, True, True, False, False),
        (True, False, False, True, False, False),
        (False, True, False, False, False, False),
    ],
)
def test_get_alerts_group_message_body(
    has_model_errors: bool,
    has_test_failures: bool,
    has_test_warnings: bool,
    has_test_errors: bool,
    has_link: bool,
    has_env: bool,
):

    detected_at = datetime(2025, 2, 3, 13, 21, 7, tzinfo=timezone.utc)

    env = "Test Env" if has_env else None
    alerts = create_test_alerts(
        has_model_errors=has_model_errors,
        has_test_failures=has_test_failures,
        has_test_warnings=has_test_warnings,
        has_test_errors=has_test_errors,
        detected_at=detected_at,
        count=10,
        env=env,
    )

    # Create a list of patches for all alerts
    patches = [
        patch.object(alert, "get_report_link", lambda: get_mock_report_link(has_link))
        for alert in alerts
    ]

    # Apply all patches
    for patcher in patches:
        patcher.start()

    alerts_group = AlertsGroup(alerts=alerts, env=env)

    message_body = get_alert_message_body(alerts_group)
    adaptive_card_filename = (
        "alerts_group"
        f"_model-errors-{has_model_errors}"
        f"_test-failures-{has_test_failures}"
        f"_test-warnings-{has_test_warnings}"
        f"_test-errors-{has_test_errors}"
        f"_link-{has_link}"
        f"_env-{has_env}"
    )
    assert_expected_json_on_all_formats(adaptive_card_filename, message_body)

    # Stop all patches
    for patcher in patches:
        patcher.stop()
