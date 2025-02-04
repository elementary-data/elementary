import itertools
from datetime import datetime
from pathlib import Path
from typing import Any, List

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from tests.unit.alerts.alert_messages.test_alert_utils import (
    BOOLEAN_VALUES,
    STATUS_VALUES,
    build_base_model_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# Generate all combinations of test parameters
params: List[List[Any]] = [
    STATUS_VALUES,  # status
    BOOLEAN_VALUES,  # has_link
    BOOLEAN_VALUES,  # has_message
    BOOLEAN_VALUES,  # has_tags
    BOOLEAN_VALUES,  # has_owners
    BOOLEAN_VALUES,  # has_path
    BOOLEAN_VALUES,  # has_suppression_interval
]
combinations = list(itertools.product(*params))


@pytest.mark.parametrize(
    "status,has_link,has_message,has_tags,has_owners,has_path,has_suppression_interval",
    combinations,
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
):
    path = "models/test_snapshot.sql" if has_path else ""
    snapshot_alert_model = build_base_model_alert_model(
        status=status,
        tags=["tag1", "tag2"] if has_tags else None,
        owners=["owner1", "owner2"] if has_owners else None,
        path=path,
        materialization="snapshot",  # Always snapshot for this test
        full_refresh=False,
        detected_at=datetime(2025, 2, 3, 13, 21, 7),
        alias="test_snapshot",
        message=("Test message" if has_message else None),
        suppression_interval=24 if has_suppression_interval else None,
    )

    monkeypatch.setattr(
        snapshot_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_alert_message_body(snapshot_alert_model)
    adaptive_card_filename = f"adaptive_card_snapshot_alert_status-{status}_link-{has_link}_message-{has_message}_tags-{has_tags}_owners-{has_owners}_path-{has_path}_suppression-{has_suppression_interval}.json"
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
