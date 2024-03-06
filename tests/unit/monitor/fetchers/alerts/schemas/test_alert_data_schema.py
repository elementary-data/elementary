import json
from datetime import datetime

import pytest

from elementary.monitor.fetchers.alerts.schema.alert_data import (
    BaseAlertDataSchema,
    TestAlertDataSchema,
)
from elementary.utils.time import DATETIME_FORMAT

CURRENT_DATETIME_UTC = datetime.utcnow()
CURRENT_TIMESTAMP_UTC = CURRENT_DATETIME_UTC.strftime(DATETIME_FORMAT)

BASE_ALERT = dict(
    id="id_1",
    alert_class_id="class_id_1",
    model_unique_id="model_id_1",
    detected_at=CURRENT_TIMESTAMP_UTC,
    database_name="db",
    schema_name="schema",
    tags=[],
    model_meta=dict(),
    suppression_status="pending",
    sent_at=None,
    status="ERROR",
)
TEST_ALERT = dict(
    **BASE_ALERT,
    test_unique_id="id_1",
    test_type="dbt_test",
    test_sub_type="general",
    test_results_description="bla",
    test_name="test_1",
    test_short_name="test",
    severity="warning",
    test_meta=dict(),
    elementary_unique_id="e_id_1",
)


def test_flatten_meta():
    flatten_meta = BaseAlertDataSchema._flatten_meta()
    assert isinstance(flatten_meta, dict)
    assert len(flatten_meta) == 0

    flatten_meta = BaseAlertDataSchema._flatten_meta(dict(a="a"))
    assert json.dumps(flatten_meta, sort_keys=True) == json.dumps(dict(a="a"), sort_keys=True)

    flatten_meta = BaseAlertDataSchema._flatten_meta(dict(a="a", alerts_config=dict(a="b")))
    assert json.dumps(flatten_meta, sort_keys=True) == json.dumps(dict(a="b"), sort_keys=True)


@pytest.mark.parametrize(
    "model_meta,test_meta,expected",
    [
        (dict(attr="a"), None, ["a"]),
        (dict(attr="a, b"), None, ["a", "b"]),
        (dict(), None, []),
        (dict(attr=["a"]), None, ["a"]),
        (dict(attr=["a", "b"]), None, ["a", "b"]),
        (dict(attr=["a", "b", "b"]), None, ["a", "b"]),
        (dict(attr="a"), dict(attr="b"), ["a", "b"]),
    ],
)
def test_get_alert_meta_attrs(model_meta, test_meta, expected):
    alert = (
        BaseAlertDataSchema(**{**BASE_ALERT, "model_meta": model_meta})
        if test_meta is None
        else TestAlertDataSchema(**{**TEST_ALERT, "model_meta": model_meta, "test_meta": test_meta})
    )
    assert sorted(alert._get_alert_meta_attrs("attr")) == sorted(expected)


def test_get_suppression_interval():
    base_alert = BaseAlertDataSchema(**{**BASE_ALERT, "model_meta": dict(alert_suppression_interval=1)})
    assert base_alert.get_suppression_interval(interval_from_cli=2, override_by_cli=False) == 1
    assert base_alert.get_suppression_interval(interval_from_cli=2, override_by_cli=True) == 2

    base_alert = BaseAlertDataSchema(**BASE_ALERT)
    assert base_alert.get_suppression_interval(interval_from_cli=2, override_by_cli=False) == 2
    assert base_alert.get_suppression_interval(interval_from_cli=2, override_by_cli=True) == 2


def test_tags():
    base_alert = BaseAlertDataSchema(**BASE_ALERT)
    assert base_alert.tags == []

    base_alert = BaseAlertDataSchema(**{**BASE_ALERT, "tags": ["a", "b"]})
    assert sorted(base_alert.tags) == sorted(["a", "b"])

    base_alert = BaseAlertDataSchema(**{**BASE_ALERT, "tags": '["a", "b"]'})
    assert sorted(base_alert.tags) == sorted(["a", "b"])

    base_alert = BaseAlertDataSchema(**{**BASE_ALERT, "tags": "a, b"})
    assert sorted(base_alert.tags) == sorted(["a", "b"])

    base_alert = BaseAlertDataSchema(**{**BASE_ALERT, "tags": "a"})
    assert sorted(base_alert.tags) == sorted(["a"])
