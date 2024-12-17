from elementary.monitor.alerts.alerts_groups.alerts_group import AlertsGroup
from elementary.monitor.alerts.test_alert import TestAlertModel
from elementary.monitor.data_monitoring.alerts.integrations.base_integration import (
    BaseIntegration,
)


def test_group_alerts():
    grouped_alerts = BaseIntegration._group_alerts(alerts=[], threshold=0)
    assert len(grouped_alerts) == 0
    grouped_alerts = BaseIntegration._group_alerts(alerts=[], threshold=1)
    assert len(grouped_alerts) == 0

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
    grouped_alerts = BaseIntegration._group_alerts(alerts=alerts, threshold=0)
    assert len(grouped_alerts) == 1
    assert isinstance(grouped_alerts[0], AlertsGroup)
    assert grouped_alerts[0].alerts == alerts
    grouped_alerts = BaseIntegration._group_alerts(alerts=alerts, threshold=1)
    assert len(grouped_alerts) == 1
    assert isinstance(grouped_alerts[0], AlertsGroup)
    assert grouped_alerts[0].alerts == alerts

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
            severity="WARN",
            test_type="dbt_test",
            test_sub_type="generic",
            test_short_name="2",
            alert_class_id="2",
        ),
    ]
    grouped_alerts = BaseIntegration._group_alerts(alerts=alerts, threshold=0)
    assert len(grouped_alerts) == 1
    assert isinstance(grouped_alerts[0], AlertsGroup)
    assert grouped_alerts[0].alerts == alerts
    grouped_alerts = BaseIntegration._group_alerts(alerts=alerts, threshold=2)
    assert len(grouped_alerts) == 1
    assert isinstance(grouped_alerts[0], AlertsGroup)
    assert grouped_alerts[0].alerts == alerts
    grouped_alerts = BaseIntegration._group_alerts(alerts=alerts, threshold=10)
    assert len(grouped_alerts) == 2
    assert grouped_alerts == alerts
