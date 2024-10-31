import pytest

from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.test_alert import TestAlertModel
from tests.mocks.data_monitoring.alerts.integrations.alerts_data_mock import (
    AlertsDataMock,
)
from tests.mocks.data_monitoring.alerts.integrations.base_integration_mock import (
    BaseIntegrationMock,
)


@pytest.fixture
def base_integration_mock() -> BaseIntegrationMock:
    return BaseIntegrationMock()


@pytest.fixture
def alerts_data_mock() -> AlertsDataMock:
    return AlertsDataMock()


def test_group_alerts(base_integration_mock: BaseIntegrationMock):
    assert (
        base_integration_mock._group_alerts(alerts=[], threshold=1) == []
    ), "Empty list should return empty list"

    alerts = [
        TestAlertModel(
            id="1",
            test_unique_id="1",
            elementary_unique_id="1",
            test_name="1",
            severity="WARN",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="1",
            alert_class_id="1",
        )
    ]
    grouped = base_integration_mock._group_alerts(alerts=alerts, threshold=2)
    assert len(grouped) == 1, "Should return one group"
    assert type(grouped[0]) is TestAlertModel, "Should be the same alert"
    grouped = base_integration_mock._group_alerts(alerts=alerts, threshold=1)
    assert len(grouped) == 1, "Should return one group"
    assert type(grouped[0]) is AlertsGroup, "Group should contain alerts"

    alerts = [
        TestAlertModel(
            id="1",
            test_unique_id="1",
            elementary_unique_id="1",
            test_name="1",
            severity="WARN",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="1",
            alert_class_id="1",
        ),
        TestAlertModel(
            id="2",
            test_unique_id="2",
            elementary_unique_id="2",
            test_name="2",
            severity="ERROR",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="2",
            alert_class_id="2",
        ),
        TestAlertModel(
            id="3",
            test_unique_id="3",
            elementary_unique_id="3",
            test_name="3",
            severity="ERROR",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="3",
            alert_class_id="3",
        ),
    ]
    grouped = base_integration_mock._group_alerts(alerts=alerts, threshold=2)
    assert len(grouped) == 1, "Should return one group"
    assert type(grouped[0]) is AlertsGroup, "Group should contain alerts"
    assert len(grouped[0].alerts) == 3, "Group should contain all alerts"
    grouped = base_integration_mock._group_alerts(alerts=alerts, threshold=5)
    assert len(grouped) == 3, "Should return three alerts"
    assert type(grouped[0]) is TestAlertModel
    assert type(grouped[1]) is TestAlertModel
    assert type(grouped[2]) is TestAlertModel


def test_get_alert_template(
    base_integration_mock: BaseIntegrationMock, alerts_data_mock: AlertsDataMock
):
    assert (
        base_integration_mock._get_alert_template(alert=alerts_data_mock.dbt_test)
        == "dbt_test"
    )
    assert (
        base_integration_mock._get_alert_template(
            alert=alerts_data_mock.elementary_test
        )
        == "elementary_test"
    )
    assert (
        base_integration_mock._get_alert_template(alert=alerts_data_mock.model)
        == "model"
    )
    assert (
        base_integration_mock._get_alert_template(alert=alerts_data_mock.snapshot)
        == "snapshot"
    )
    assert (
        base_integration_mock._get_alert_template(
            alert=alerts_data_mock.source_freshness
        )
        == "source_freshness"
    )
    assert (
        base_integration_mock._get_alert_template(
            alert=alerts_data_mock.grouped_by_table
        )
        == "grouped_by_table"
    )
