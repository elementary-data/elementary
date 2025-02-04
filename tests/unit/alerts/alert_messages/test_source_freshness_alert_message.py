import itertools
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence, Union

import pytest

from elementary.messages.formats.adaptive_cards import format_adaptive_card
from tests.unit.alerts.alert_messages.test_alert_utils import (
    BOOLEAN_VALUES,
    build_base_source_freshness_alert_model,
    get_alert_message_body,
    get_mock_report_link,
)
from tests.unit.messages.utils import assert_expected_json, get_expected_json_path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

SOURCE_FRESHNESS_STATUS_VALUES = [
    "runtime error",
    "error",
]

# Generate all combinations of test parameters
params: Sequence[Sequence[Union[Optional[str], bool]]] = [
    SOURCE_FRESHNESS_STATUS_VALUES,  # status
    BOOLEAN_VALUES,  # has_link
    BOOLEAN_VALUES,  # has_message
    BOOLEAN_VALUES,  # has_tags
    BOOLEAN_VALUES,  # has_owners
    BOOLEAN_VALUES,  # has_path
    BOOLEAN_VALUES,  # has_error
    BOOLEAN_VALUES,  # has_suppression_interval
]
combinations = list(itertools.product(*params))


@pytest.mark.parametrize(
    "status,has_link,has_message,has_tags,has_owners,has_path,has_error,has_suppression_interval",
    combinations,
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
):
    path = "sources/test_source.yml" if has_path else ""
    detected_at = datetime(2025, 2, 3, 13, 21, 7)
    snapshotted_at = detected_at
    max_loaded_at = datetime(2025, 2, 3, 11, 21, 7) if has_message else None
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
    )

    monkeypatch.setattr(
        source_freshness_alert_model,
        "get_report_link",
        lambda: get_mock_report_link(has_link),
    )

    message_body = get_alert_message_body(source_freshness_alert_model)
    adaptive_card_filename = f"adaptive_card_source_freshness_alert_status-{status}_link-{has_link}_message-{has_message}_tags-{has_tags}_owners-{has_owners}_path-{has_path}_error-{has_error}_suppression-{has_suppression_interval}.json"
    adaptive_card_json = format_adaptive_card(message_body)
    expected_adaptive_card_json_path = get_expected_json_path(
        FIXTURES_DIR, adaptive_card_filename
    )
    assert_expected_json(adaptive_card_json, expected_adaptive_card_json_path)
