import json

import pytest

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.api.alerts.schema import AlertsSchema
from elementary.monitor.data_monitoring.alerts.integrations.slack.slack import (
    SlackIntegration,
)
from tests.mocks.api.alerts_api_mock import MockAlertsAPI
from tests.mocks.data_monitoring.data_monitoring_alerts_mock import (
    DataMonitoringAlertsMock,
)


def test_get_integration_client(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    integration = data_monitoring_alerts_mock._get_integration_client()
    assert isinstance(integration, SlackIntegration)


def test_fetch_data(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    mock_alerts_api = MockAlertsAPI()
    alerts_data = data_monitoring_alerts_mock._fetch_data(days_back=1)
    assert isinstance(alerts_data, AlertsSchema)
    assert alerts_data.json(sort_keys=True) == mock_alerts_api.get_new_alerts(
        days_back=1
    ).json(sort_keys=True)


def test_format_alerts(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    alerts = data_monitoring_alerts_mock._fetch_data(days_back=1)
    formatted_alerts = data_monitoring_alerts_mock._format_alerts(alerts)
    test_alerts = [
        alert for alert in formatted_alerts if isinstance(alert, TestAlertModel)
    ]
    model_alerts = [
        alert for alert in formatted_alerts if isinstance(alert, ModelAlertModel)
    ]
    source_freshness_alerts = [
        alert
        for alert in formatted_alerts
        if isinstance(alert, SourceFreshnessAlertModel)
    ]
    assert json.dumps(
        [alert.id for alert in test_alerts], sort_keys=True
    ) == json.dumps(["alert_id_2", "alert_id_3", "alert_id_4"])
    assert json.dumps(
        [alert.id for alert in model_alerts], sort_keys=True
    ) == json.dumps(["alert_id_2", "alert_id_3", "alert_id_4"])
    assert json.dumps(
        [alert.id for alert in source_freshness_alerts], sort_keys=True
    ) == json.dumps(["alert_id_2", "alert_id_3", "alert_id_4"])

    # test format group by table
    data_monitoring_alerts_mock.config.slack_group_alerts_by = "table"
    formatted_alerts = data_monitoring_alerts_mock._format_alerts(alerts)
    sorted_formatted_alerts = sorted(
        formatted_alerts, key=lambda alert: alert.model_unique_id
    )
    grouped_by_table_ids = [alert.model_unique_id for alert in sorted_formatted_alerts]
    assert grouped_by_table_ids == [
        "model_id_1",
        "model_id_2",
        "model_id_3",
        "model_id_4",
        "source_id_2",
        "source_id_3",
        "source_id_4",
    ]
    assert len(sorted_formatted_alerts[0].alerts) == 1
    assert len(sorted_formatted_alerts[1].alerts) == 3
    assert len(sorted_formatted_alerts[2].alerts) == 1
    assert len(sorted_formatted_alerts[3].alerts) == 1
    assert len(sorted_formatted_alerts[4].alerts) == 1
    assert len(sorted_formatted_alerts[5].alerts) == 1
    assert len(sorted_formatted_alerts[6].alerts) == 1


def test_test_format(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    alerts = data_monitoring_alerts_mock._fetch_data(days_back=1)

    # format test alerts
    for alert in alerts.tests.send + alerts.tests.skip:
        assert isinstance(
            data_monitoring_alerts_mock._format_alert(alert), TestAlertModel
        )

    # format model alerts
    for alert in alerts.models.send + alerts.models.skip:
        assert isinstance(
            data_monitoring_alerts_mock._format_alert(alert), ModelAlertModel
        )

    # format source_freshness alerts
    for alert in alerts.source_freshnesses.send + alerts.source_freshnesses.skip:
        assert isinstance(
            data_monitoring_alerts_mock._format_alert(alert), SourceFreshnessAlertModel
        )


@pytest.fixture
def data_monitoring_alerts_mock() -> DataMonitoringAlertsMock:
    return DataMonitoringAlertsMock()
