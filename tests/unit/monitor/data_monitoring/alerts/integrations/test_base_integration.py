import pytest

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
