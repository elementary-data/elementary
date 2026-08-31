"""
Regression tests for issue #2304:
AlertModel must treat naive detected_at values as UTC, not local time.
"""
from datetime import datetime, timezone as dt_timezone

import pytest

from elementary.monitor.alerts.alert import AlertModel


def _make_alert(detected_at, timezone=None):
    return AlertModel(
        id="test",
        alert_class_id="cls",
        detected_at=detected_at,
        timezone=timezone,
    )


def test_naive_detected_at_treated_as_utc_not_local():
    """A naive datetime must be interpreted as UTC, not as local wall-clock time.

    Regression: alert.detected_at_utc must equal the naive input with UTC tzinfo
    attached, regardless of the process timezone.
    """
    naive_utc = datetime(2026, 7, 22, 9, 8, 22)
    alert = _make_alert(naive_utc, timezone="Asia/Tokyo")

    expected_utc = naive_utc.replace(tzinfo=dt_timezone.utc)
    assert alert.detected_at_utc == expected_utc, (
        "detected_at_utc must be the naive input with UTC tzinfo, "
        f"got {alert.detected_at_utc!r}"
    )

    # After conversion to JST (+09:00) the hour must be 18 (9 + 9)
    assert alert.detected_at is not None
    assert alert.detected_at.hour == 18, (
        "09:08:22 UTC converted to JST (+09:00) must be 18:08:22, "
        f"got {alert.detected_at}"
    )


def test_aware_detected_at_is_not_re_interpreted():
    """An already-aware datetime must be used as-is (no double-conversion)."""
    aware_utc = datetime(2026, 7, 22, 9, 8, 22, tzinfo=dt_timezone.utc)
    alert = _make_alert(aware_utc, timezone="Asia/Tokyo")

    assert alert.detected_at_utc == aware_utc
    assert alert.detected_at is not None
    assert alert.detected_at.hour == 18


def test_none_detected_at_leaves_fields_none():
    alert = _make_alert(None)
    assert alert.detected_at is None
    assert alert.detected_at_utc is None
    assert alert.detected_at_str == "N/A"
