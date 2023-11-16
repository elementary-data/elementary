import json
from typing import Optional
from unittest import mock

from tests.mocks.data_monitoring.alerts.integrations.alerts_data_mock import (
    DBT_TEST_ALERT_MOCK,
    AlertsDataMock,
)
from tests.mocks.data_monitoring.alerts.integrations.slack_integration_mock import (
    SlackIntegrationMock,
)


def test_get_alert_template_workflow_param():
    alerts_data = AlertsDataMock()

    workflow_integration = get_slack_integration_mock(
        is_slack_workflow=True, slack_token="mock", slack_channel_name="mock"
    )
    assert workflow_integration._get_alert_template(
        alert=alerts_data.dbt_test
    ).text == json.dumps(alerts_data.dbt_test.data, sort_keys=True)

    integration = get_slack_integration_mock(
        slack_token="mock", slack_channel_name="mock"
    )
    integration._get_dbt_test_template = mock.Mock(return_value="dbt_test")
    assert integration._get_alert_template(alert=alerts_data.dbt_test) == "dbt_test"


def test_get_integration_params_different_slack_clients_logic():
    alerts_data = AlertsDataMock()

    webhook_client_integration = get_slack_integration_mock(slack_webhook="mock")
    assert json.dumps(
        webhook_client_integration._get_integration_params(alert=alerts_data.dbt_test)
    ) == json.dumps({})

    web_client_integration = get_slack_integration_mock(
        slack_token="mock", slack_channel_name="mock"
    )
    assert json.dumps(
        web_client_integration._get_integration_params(alert=alerts_data.dbt_test)
    ) == json.dumps({"channel": "mock"})


def test_get_integration_params_channel_logic():
    # alert without channel defined in meta
    alert = DBT_TEST_ALERT_MOCK
    integration = get_slack_integration_mock(
        slack_token="mock", slack_channel_name="from_conf"
    )
    assert json.dumps(integration._get_integration_params(alert=alert)) == json.dumps(
        {"channel": "from_conf"}
    )

    # alert with channel defined
    alert = DBT_TEST_ALERT_MOCK
    alert.model_meta.update(dict(channel="from_alert"))
    integration = get_slack_integration_mock(
        slack_token="mock", slack_channel_name="from_conf"
    )
    assert json.dumps(integration._get_integration_params(alert=alert)) == json.dumps(
        {"channel": "from_alert"}
    )

    # alert with channel defined and override config as True
    alert = DBT_TEST_ALERT_MOCK
    alert.model_meta.update(dict(channel="from_alert"))
    integration = get_slack_integration_mock(
        slack_token="mock",
        slack_channel_name="from_conf",
        override_config_defaults=True,
    )
    assert json.dumps(integration._get_integration_params(alert=alert)) == json.dumps(
        {"channel": "from_conf"}
    )


def get_slack_integration_mock(
    override_config_defaults: bool = False,
    is_slack_workflow: bool = False,
    slack_token: Optional[str] = None,
    slack_channel_name: Optional[str] = None,
    slack_webhook: Optional[str] = None,
) -> SlackIntegrationMock:
    return SlackIntegrationMock(
        override_config_defaults=override_config_defaults,
        is_slack_workflow=is_slack_workflow,
        slack_token=slack_token,
        slack_channel_name=slack_channel_name,
        slack_webhook=slack_webhook,
    )
