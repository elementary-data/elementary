import itertools
from datetime import datetime
from pathlib import Path
from typing import Any, List

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from elementary.monitor.alerts.alert_messages.test_alert_message import (
    get_snapshot_alert_message_body,
)
from elementary.monitor.alerts.model_alert import ModelAlertModel
from tests.unit.alerts.alert_messages.test_alert_utils import (
    BOOLEAN_VALUES,
    STATUS_VALUES,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def build_snapshot_alert_model(
    status: str,
    has_message: bool,
    has_tags: bool,
    has_owners: bool,
    has_path: bool,
    has_suppression_interval: bool,
) -> ModelAlertModel:
    path = "models/test_snapshot.sql" if has_path else ""
    return ModelAlertModel(
        id="test_id",
        alias="test_snapshot",
        path=path,
        original_path=path,
        materialization="snapshot",
        full_refresh=False,
        alert_class_id="test_alert_class_id",
        message="Test message" if has_message else None,
        model_unique_id="test_model_unique_id",
        detected_at=datetime(2025, 2, 3, 13, 21, 7),
        owners=["owner1", "owner2"] if has_owners else None,
        tags=["tag1", "tag2"] if has_tags else None,
        subscribers=None,
        status=status,
        model_meta={},
        suppression_interval=24 if has_suppression_interval else None,
        timezone="UTC",
        report_url=None,
        alert_fields=None,
    )


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
    snapshot_alert_model = build_snapshot_alert_model(
        status=status,
        has_message=has_message,
        has_tags=has_tags,
        has_owners=has_owners,
        has_path=has_path,
        has_suppression_interval=has_suppression_interval,
    )

    monkeypatch.setattr(
        snapshot_alert_model, "get_report_link", lambda: get_mock_report_link(has_link)
    )

    message_body = get_snapshot_alert_message_body(snapshot_alert_model)
    adaptive_card_filename = f"adaptive_card_snapshot_alert_status-{status}_link-{has_link}_message-{has_message}_tags-{has_tags}_owners-{has_owners}_path-{has_path}_suppression-{has_suppression_interval}.json"
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
