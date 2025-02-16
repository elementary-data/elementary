from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.unit.alerts.alert_messages.test_alert_utils import (
    assert_expected_json_on_all_formats,
    build_base_source_freshness_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.mark.parametrize(
    "status,has_link,has_message,has_tags,has_owners,has_path,has_error,has_suppression_interval,has_env",
    [
        ("runtime error", False, False, False, False, False, False, False, False),
        ("runtime error", True, True, True, True, True, True, True, True),
        ("runtime error", True, True, False, True, False, True, False, True),
        ("runtime error", True, False, True, False, True, False, True, True),
        ("runtime error", False, True, True, False, False, True, True, True),
        ("error", False, True, True, True, False, True, False, True),
        ("error", True, False, True, True, True, False, True, False),
        ("error", True, True, False, False, True, True, True, True),
        ("error", False, False, True, True, True, True, False, False),
        ("error", True, False, False, False, False, False, True, False),
    ],
)
def test_get_source_freshness_alert_message_body(
    monkeypatch,
    status: str,
    has_link: bool,
    has_message: bool,
    has_tags: bool,
    has_owners: bool,
    has_path: bool,
    has_error: bool,
    has_suppression_interval: bool,
    has_env: bool,
):
    env = "Test Env" if has_env else None
    path = "sources/test_source.yml" if has_path else ""
    detected_at = datetime(2025, 2, 3, 13, 21, 7, tzinfo=timezone.utc)
    snapshotted_at = detected_at
    max_loaded_at = (
        datetime(2025, 2, 3, 11, 21, 7, tzinfo=timezone.utc) if has_message else None
    )
    max_loaded_at_time_ago_in_s = 7200.0 if has_message else None

    source_freshness_alert_model = build_base_source_freshness_alert_model(
        status=status,
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        path=path,
        has_error=has_error,
        has_message=has_message,
        detected_at=detected_at,
        snapshotted_at=snapshotted_at,
        max_loaded_at=max_loaded_at,
        max_loaded_at_time_ago_in_s=max_loaded_at_time_ago_in_s,
        suppression_interval=24 if has_suppression_interval else None,
        env=env,
    )

    monkeypatch.setattr(
        source_freshness_alert_model,
        "get_report_link",
        lambda: get_mock_report_link(has_link),
    )

    message_body = get_alert_message_body(source_freshness_alert_model)
    filename = (
        f"source_freshness_alert"
        f"_status-{status}"
        f"_link-{has_link}"
        f"_message-{has_message}"
        f"_tags-{has_tags}"
        f"_owners-{has_owners}"
        f"_path-{has_path}"
        f"_error-{has_error}"
        f"_suppression-{has_suppression_interval}"
        f"_env-{has_env}"
    )
    assert_expected_json_on_all_formats(filename, message_body)
