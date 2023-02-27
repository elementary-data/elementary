from elementary.monitor.alerts.group_of_alerts import GroupOfAlerts
from tests.unit.monitor.alerts.group_alerts_by_table.mock_classes import (
    MockDataMonitoringAlerts,
)


def mock_data_monitoring_alerts(mock_config):
    return MockDataMonitoringAlerts(config=mock_config, execution_properties=dict())


def check_eq_group_alerts(grp1: GroupOfAlerts, grp2: GroupOfAlerts):
    ret = grp1.alerts == grp2.alerts  # same alerts
    ret &= grp1._components_to_alerts == grp2._components_to_alerts  # same mappings
    ret &= (
        grp1._components_to_attention_required == grp2._components_to_attention_required
    )  # same owners, tags, subscribers

    return ret
