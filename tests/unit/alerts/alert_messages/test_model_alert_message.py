from datetime import datetime
from pathlib import Path

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from tests.unit.alerts.alert_messages.test_alert_utils import (
    build_base_model_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.mark.parametrize(
    "status,has_link,has_tags,has_owners,has_path,has_suppression_interval,materialization,has_full_refresh,has_env",
    [
        ("fail", True, True, True, True, True, "table", True, True),
        ("fail", False, False, False, False, False, "table", False, False),
        ("warn", True, True, True, False, True, "view", True, True),
        ("error", False, True, True, True, False, "incremental", True, False),
        (None, True, False, True, True, True, "table", False, True),
        ("warn", False, True, False, True, True, "incremental", False, False),
        ("error", True, True, False, False, True, "view", True, True),
        (None, False, False, True, False, True, "table", True, True),
        ("fail", True, False, False, True, False, "incremental", True, False),
        ("warn", False, True, True, True, True, "view", False, False),
    ],
)
def test_get_model_alert_message_body(
    monkeypatch,
    status: str,
    has_link: bool,
    has_tags: bool,
    has_owners: bool,
    has_path: bool,
    has_suppression_interval: bool,
    materialization: str,
    has_full_refresh: bool,
    has_env: bool,
):
    env = "Test Env" if has_env else None
    path = "models/test_model.sql" if has_path else "unknown_path"
    model_alert_model = build_base_model_alert_model(
        status=status,
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        path=path,
        materialization=materialization,
        full_refresh=has_full_refresh,
        detected_at=datetime(2025, 2, 3, 13, 21, 7),
        alias="test_model",
        message=None,
        suppression_interval=24 if has_suppression_interval else None,
        env=env,
    )

    monkeypatch.setattr(
        model_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_alert_message_body(model_alert_model)
    adaptive_card_filename = (
        f"adaptive_card_model_alert"
        f"_status-{status}"
        f"_link-{has_link}"
        f"_tags-{has_tags}"
        f"_owners-{has_owners}"
        f"_path-{has_path}"
        f"_suppression-{has_suppression_interval}"
        f"_materialization-{materialization}"
        f"_full_refresh-{has_full_refresh}"
        f"_env-{has_env}"
        ".json"
    )
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
