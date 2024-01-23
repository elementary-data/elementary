import json

import pytest

from tests.mocks.api.alerts_api_mock import MockAlertsAPI


def test_get_suppressed_alerts(alerts_api_mock: MockAlertsAPI):
    last_alert_sent_times = alerts_api_mock.alerts_fetcher.query_last_alert_times()

    alerts = alerts_api_mock.alerts_fetcher.query_pending_alerts()
    test_alerts = [alert for alert in alerts if alert.type == "test"]
    model_alerts = [alert for alert in alerts if alert.type == "model"]

    suppressed_test_alerts = alerts_api_mock._get_suppressed_alerts(
        test_alerts,
        last_alert_sent_times,
    )
    suppressed_model_alerts = alerts_api_mock._get_suppressed_alerts(
        model_alerts,
        last_alert_sent_times,
    )

    assert json.dumps(suppressed_test_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1"], sort_keys=True
    )
    assert json.dumps(suppressed_model_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1"], sort_keys=True
    )


def test_sort_alerts(alerts_api_mock: MockAlertsAPI):
    last_alert_sent_times = alerts_api_mock.alerts_fetcher.query_last_alert_times()

    alerts = alerts_api_mock.alerts_fetcher.query_pending_alerts()
    test_alerts = [alert for alert in alerts if alert.type == "test"]
    model_alerts = [alert for alert in alerts if alert.type == "model"]

    sorted_test_alerts = alerts_api_mock._sort_alerts(
        test_alerts,
        last_alert_sent_times,
    )
    sorted_model_alerts = alerts_api_mock._sort_alerts(
        model_alerts,
        last_alert_sent_times,
    )

    # alert_id_1 is suppressed and alert_id_5 is duplicated
    assert [alert.id for alert in sorted_test_alerts.skip].sort() == [
        "alert_id_1",
        "alert_id_5",
    ].sort()
    assert [alert.id for alert in sorted_test_alerts.send].sort() == [
        "alert_id_2",
        "alert_id_3",
        "alert_id_4",
    ].sort()
    # alert_id_1 is suppressed and alert_id_5 is duplicated
    assert [alert.id for alert in sorted_model_alerts.skip].sort() == [
        "alert_id_1",
        "alert_id_5",
    ].sort()
    assert [alert.id for alert in sorted_model_alerts.send].sort() == [
        "alert_id_2",
        "alert_id_3",
        "alert_id_4",
    ].sort()


def test_get_latest_alerts(alerts_api_mock: MockAlertsAPI):
    alerts = alerts_api_mock.alerts_fetcher.query_pending_alerts()
    test_alerts = [alert for alert in alerts if alert.type == "test"]
    model_alerts = [alert for alert in alerts if alert.type == "model"]

    latest_test_alerts = alerts_api_mock._get_latest_alerts(test_alerts)
    latest_model_alerts = alerts_api_mock._get_latest_alerts(model_alerts)

    # Only alert_id_5 is a duplicate alert (of alert_id_4)
    assert json.dumps(latest_test_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1", "alert_id_2", "alert_id_3", "alert_id_4"], sort_keys=True
    )
    # Only alert_id_5 is a duplicate alert (of alert_id_4)
    assert json.dumps(latest_model_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1", "alert_id_2", "alert_id_3", "alert_id_4"], sort_keys=True
    )


@pytest.fixture
def alerts_api_mock() -> MockAlertsAPI:
    return MockAlertsAPI()
