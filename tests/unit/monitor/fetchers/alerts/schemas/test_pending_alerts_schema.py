from datetime import datetime

import pytest

from elementary.monitor.fetchers.alerts.schema.pending_alerts import (
    AlertTypes,
    PendingAlertSchema,
)
from tests.mocks.fetchers.alerts_fetcher_mock import (
    PENDDING_SOURCE_FRESHNESS_ALERTS_MOCK_DATA,
)


def test_detected_at_none(pending_alert_data: dict):
    pending_alert = PendingAlertSchema(
        id=pending_alert_data["id"],
        alert_class_id=pending_alert_data["alert_class_id"],
        type=AlertTypes.SOURCE_FRESHNESS,
        detected_at=None,
        created_at=pending_alert_data["detected_at"],
        updated_at=pending_alert_data["detected_at"],
        status="pending",
        data=pending_alert_data,
    )
    assert isinstance(pending_alert.detected_at, datetime)


def test_created_at_none(pending_alert_data: dict):
    pending_alert = PendingAlertSchema(
        id=pending_alert_data["id"],
        alert_class_id=pending_alert_data["alert_class_id"],
        type=AlertTypes.SOURCE_FRESHNESS,
        detected_at=pending_alert_data["detected_at"],
        created_at=None,
        updated_at=pending_alert_data["detected_at"],
        status="pending",
        data=pending_alert_data,
    )
    assert isinstance(pending_alert.created_at, datetime)


def test_updated_at_none(pending_alert_data: dict):
    pending_alert = PendingAlertSchema(
        id=pending_alert_data["id"],
        alert_class_id=pending_alert_data["alert_class_id"],
        type=AlertTypes.SOURCE_FRESHNESS,
        detected_at=pending_alert_data["detected_at"],
        created_at=pending_alert_data["detected_at"],
        updated_at=None,
        status="pending",
        data=pending_alert_data,
    )
    assert isinstance(pending_alert.updated_at, datetime)


@pytest.fixture
def pending_alert_data() -> dict:
    return PENDDING_SOURCE_FRESHNESS_ALERTS_MOCK_DATA[0]
