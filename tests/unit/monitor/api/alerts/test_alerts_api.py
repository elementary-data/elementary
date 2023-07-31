import json
from typing import Optional

import pytest
from parametrization import Parametrization

from tests.mocks.api.alerts_api_mock import MockAlertsAPI


def test_get_suppressed_alerts(alerts_api_mock: MockAlertsAPI):
    last_test_alert_sent_times = (
        alerts_api_mock.alerts_fetcher.query_last_test_alert_times()
    )
    last_model_alert_sent_times = (
        alerts_api_mock.alerts_fetcher.query_last_model_alert_times()
    )

    test_alerts = alerts_api_mock.alerts_fetcher.query_pending_test_alerts()
    model_alerts = alerts_api_mock.alerts_fetcher.query_pending_model_alerts()

    suppressed_test_alerts = alerts_api_mock._get_suppressed_alerts(
        test_alerts,
        last_test_alert_sent_times,
    )
    suppressed_model_alerts = alerts_api_mock._get_suppressed_alerts(
        model_alerts,
        last_model_alert_sent_times,
    )

    assert json.dumps(suppressed_test_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1"], sort_keys=True
    )
    assert json.dumps(suppressed_model_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1"], sort_keys=True
    )


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="meta is none- cli wins",
    cli_interval=1,
    alert_interval=None,
    expected_interval=1,
    override_suppression_interval=False,
)
@Parametrization.case(
    name="meta is not none- meta wins",
    cli_interval=2,
    alert_interval=10,
    expected_interval=10,
    override_suppression_interval=False,
)
@Parametrization.case(
    name="meta is not none but override is set- cli wins",
    cli_interval=2,
    alert_interval=10,
    expected_interval=2,
    override_suppression_interval=True,
)
def test_get_suppression_interval(
    alerts_api_mock: MockAlertsAPI,
    cli_interval: int,
    alert_interval: Optional[int],
    override_suppression_interval: bool,
    expected_interval: Optional[int],
):
    assert (
        alerts_api_mock._get_suppression_interval(
            alert_interval, cli_interval, override_suppression_interval
        )
        == expected_interval
    )


def test_get_latest_alerts(alerts_api_mock: MockAlertsAPI):
    test_alerts = alerts_api_mock.alerts_fetcher.query_pending_test_alerts()
    model_alerts = alerts_api_mock.alerts_fetcher.query_pending_model_alerts()

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
