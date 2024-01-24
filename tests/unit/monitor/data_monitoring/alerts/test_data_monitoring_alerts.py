import json
from datetime import datetime

import pytest

from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.slack.slack import (
    SlackIntegration,
)
from elementary.monitor.fetchers.alerts.schema.alert_data import (
    ModelAlertDataSchema,
    SourceFreshnessAlertDataSchema,
    TestAlertDataSchema,
)
from elementary.monitor.fetchers.alerts.schema.pending_alerts import PendingAlertSchema
from tests.mocks.api.alerts_api_mock import MockAlertsAPI
from tests.mocks.data_monitoring.alerts.data_monitoring_alerts_mock import (
    DataMonitoringAlertsMock,
)


def test_get_integration_client(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    integration = data_monitoring_alerts_mock._get_integration_client()
    assert isinstance(integration, SlackIntegration)


def test_fetch_data(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    mock_alerts_api = MockAlertsAPI()
    alerts_data = data_monitoring_alerts_mock._fetch_data(days_back=1)
    assert all(isinstance(alert_data, PendingAlertSchema) for alert_data in alerts_data)
    assert json.dumps(
        [data.json(sort_keys=True) for data in alerts_data], sort_keys=True
    ) == json.dumps(
        [
            data.json(sort_keys=True)
            for data in mock_alerts_api.get_new_alerts(days_back=1)
        ],
        sort_keys=True,
    )


def test_fetch_last_sent_times(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    mock_alerts_api = MockAlertsAPI()
    alerts_last_sent_times = data_monitoring_alerts_mock._fetch_last_sent_times(
        days_back=1
    )
    assert all(
        isinstance(sent_at, datetime) for sent_at in alerts_last_sent_times.values()
    )
    assert sorted(
        [
            f"{id}{sent_at.isoformat()}"
            for (id, sent_at) in alerts_last_sent_times.items()
        ]
    ) == sorted(
        [
            f"{id}{sent_at.isoformat()}"
            for (id, sent_at) in mock_alerts_api.get_alerts_last_sent_times(
                days_back=1
            ).items()
        ]
    )


def test_sort_alerts(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    alerts = data_monitoring_alerts_mock._fetch_data(days_back=1)
    alerts_last_sent_times = data_monitoring_alerts_mock._fetch_last_sent_times(
        days_back=1
    )
    sorted_alerts = data_monitoring_alerts_mock._sort_alerts(
        alerts, alerts_last_sent_times
    )

    assert sorted(
        [
            alert.id
            for alert in sorted_alerts.send
            if isinstance(alert.data, TestAlertDataSchema)
        ]
    ) == [
        "alert_id_2",
        "alert_id_3",
        "alert_id_4",
    ]
    assert sorted(
        [
            alert.id
            for alert in sorted_alerts.send
            if isinstance(alert.data, ModelAlertDataSchema)
        ]
    ) == [
        "alert_id_2",
        "alert_id_3",
        "alert_id_4",
    ]
    assert sorted(
        [
            alert.id
            for alert in sorted_alerts.send
            if isinstance(alert.data, SourceFreshnessAlertDataSchema)
        ]
    ) == [
        "alert_id_2",
        "alert_id_3",
        "alert_id_4",
    ]

    assert sorted(
        [
            alert.id
            for alert in sorted_alerts.skip
            if isinstance(alert.data, TestAlertDataSchema)
        ]
    ) == [
        "alert_id_1",
        "alert_id_5",
    ]
    assert sorted(
        [
            alert.id
            for alert in sorted_alerts.skip
            if isinstance(alert.data, ModelAlertDataSchema)
        ]
    ) == [
        "alert_id_1",
        "alert_id_5",
    ]
    assert sorted(
        [
            alert.id
            for alert in sorted_alerts.skip
            if isinstance(alert.data, SourceFreshnessAlertDataSchema)
        ]
    ) == [
        "alert_id_1",
        "alert_id_5",
    ]


def test_get_suppressed_alerts(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    alerts = data_monitoring_alerts_mock._fetch_data(days_back=1)
    alerts_last_sent_times = data_monitoring_alerts_mock._fetch_last_sent_times(
        days_back=1
    )
    test_alerts = [alert for alert in alerts if alert.type == "test"]
    model_alerts = [alert for alert in alerts if alert.type == "model"]

    suppressed_test_alerts = data_monitoring_alerts_mock._get_suppressed_alerts(
        test_alerts,
        alerts_last_sent_times,
    )
    suppressed_model_alerts = data_monitoring_alerts_mock._get_suppressed_alerts(
        model_alerts,
        alerts_last_sent_times,
    )

    assert json.dumps(suppressed_test_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1"], sort_keys=True
    )
    assert json.dumps(suppressed_model_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1"], sort_keys=True
    )


def test_get_latest_alerts(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    alerts = data_monitoring_alerts_mock._fetch_data(days_back=1)
    test_alerts = [alert for alert in alerts if alert.type == "test"]
    model_alerts = [alert for alert in alerts if alert.type == "model"]

    latest_test_alerts = data_monitoring_alerts_mock._get_latest_alerts(test_alerts)
    latest_model_alerts = data_monitoring_alerts_mock._get_latest_alerts(model_alerts)

    # Only alert_id_5 is a duplicate alert (of alert_id_4)
    assert json.dumps(latest_test_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1", "alert_id_2", "alert_id_3", "alert_id_4"], sort_keys=True
    )
    # Only alert_id_5 is a duplicate alert (of alert_id_4)
    assert json.dumps(latest_model_alerts, sort_keys=True) == json.dumps(
        ["alert_id_1", "alert_id_2", "alert_id_3", "alert_id_4"], sort_keys=True
    )


def test_format_alerts(data_monitoring_alerts_mock: DataMonitoringAlertsMock):
    alerts = data_monitoring_alerts_mock._fetch_data(days_back=1)
    alerts_last_sent_times = data_monitoring_alerts_mock._fetch_last_sent_times(
        days_back=1
    )
    sorted_alerts = data_monitoring_alerts_mock._sort_alerts(
        alerts, alerts_last_sent_times
    )
    formatted_alerts = data_monitoring_alerts_mock._format_alerts(sorted_alerts.send)
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
    formatted_alerts = data_monitoring_alerts_mock._format_alerts(sorted_alerts.send)
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


@pytest.fixture
def data_monitoring_alerts_mock() -> DataMonitoringAlertsMock:
    return DataMonitoringAlertsMock()
