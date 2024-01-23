import json

from elementary.monitor.fetchers.alerts.schema.pending_alerts import AlertTypes
from tests.mocks.api.alerts_api_mock import MockAlertsAPI


def test_get_new_alerts():
    api = MockAlertsAPI()
    alerts = api.get_new_alerts(days_back=1)
    alerts_to_send = alerts.send
    alerts_to_skip = alerts.skip

    # Test the following tests are not suppressed:
    #   - Alert after suppression interval
    #   - Alert without suppression interval
    #   - First occurrence alert with suppression interval
    assert json.dumps(
        [
            alert.id
            for alert in alerts_to_send
            if AlertTypes(alert.type) is AlertTypes.TEST
        ],
        sort_keys=True,
    ) == json.dumps(["alert_id_2", "alert_id_3", "alert_id_4"], sort_keys=True)
    assert json.dumps(
        [
            alert.id
            for alert in alerts_to_send
            if AlertTypes(alert.type) is AlertTypes.MODEL
        ],
        sort_keys=True,
    ) == json.dumps(["alert_id_2", "alert_id_3", "alert_id_4"], sort_keys=True)
    assert json.dumps(
        [
            alert.id
            for alert in alerts_to_send
            if AlertTypes(alert.type) is AlertTypes.SOURCE_FRESHNESS
        ],
        sort_keys=True,
    ) == json.dumps(["alert_id_2", "alert_id_3", "alert_id_4"], sort_keys=True)

    # Test the following tests are suppressed:
    #   - alert_id_1 (Alert within suppression interval)
    # Test the following tests are skipped due to dedup:
    #   - alert_id_5 (Duplication of alert_id_4 with earlier detected time)
    assert json.dumps(
        [
            alert.id
            for alert in alerts_to_skip
            if AlertTypes(alert.type) is AlertTypes.TEST
        ],
        sort_keys=True,
    ) == json.dumps(["alert_id_1", "alert_id_5"], sort_keys=True)
    assert json.dumps(
        [
            alert.id
            for alert in alerts_to_skip
            if AlertTypes(alert.type) is AlertTypes.MODEL
        ],
        sort_keys=True,
    ) == json.dumps(["alert_id_1", "alert_id_5"], sort_keys=True)
    assert json.dumps(
        [
            alert.id
            for alert in alerts_to_skip
            if AlertTypes(alert.type) is AlertTypes.SOURCE_FRESHNESS
        ],
        sort_keys=True,
    ) == json.dumps(["alert_id_1", "alert_id_5"], sort_keys=True)
