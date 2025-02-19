from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.unit.alerts.alert_messages.test_alert_utils import (
    assert_expected_json_on_all_formats,
    build_base_model_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.mark.parametrize(
    "status,has_link,has_message,has_tags,has_owners,has_path,has_suppression_interval,has_env",
    [
        ("fail", True, True, True, True, True, True, True),
        ("fail", False, False, False, False, False, False, False),
        ("warn", True, False, True, False, True, False, True),
        ("warn", False, True, False, True, False, True, False),
        ("error", True, True, False, True, True, False, True),
        ("error", False, True, True, False, False, True, False),
        (None, True, False, True, False, False, True, True),
        (None, False, False, False, True, True, False, True),
        ("fail", True, False, True, True, False, True, False),
        ("warn", False, True, True, False, True, False, False),
    ],
)
def test_get_snapshot_alert_message_body(
    monkeypatch,
    status: str,
    has_link: bool,
    has_message: bool,
    has_tags: bool,
    has_owners: bool,
    has_path: bool,
    has_suppression_interval: bool,
    has_env: bool,
):
    env = "Test Env" if has_env else None
    path = "models/test_snapshot.sql" if has_path else ""
    snapshot_alert_model = build_base_model_alert_model(
        status=status,
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        path=path,
        materialization="snapshot",  # Always snapshot for this test
        full_refresh=False,
        detected_at=datetime(2025, 2, 3, 13, 21, 7, tzinfo=timezone.utc),
        alias="test_snapshot",
        message="Test message" if has_message else None,
        suppression_interval=24 if has_suppression_interval else None,
        env=env,
    )

    monkeypatch.setattr(
        snapshot_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_alert_message_body(snapshot_alert_model)
    filename = (
        f"snapshot_alert"
        f"_status-{status}"
        f"_link-{has_link}"
        f"_message-{has_message}"
        f"_tags-{has_tags}"
        f"_owners-{has_owners}"
        f"_path-{has_path}"
        f"_suppression-{has_suppression_interval}"
        f"_env-{has_env}"
    )
    assert_expected_json_on_all_formats(filename, message_body)
