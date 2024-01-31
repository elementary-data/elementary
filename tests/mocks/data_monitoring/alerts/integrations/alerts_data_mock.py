from elementary.monitor.alerts.group_of_alerts import GroupedByTableAlerts
from elementary.monitor.alerts.model_alert import ModelAlertModel
from elementary.monitor.alerts.source_freshness_alert import SourceFreshnessAlertModel
from elementary.monitor.alerts.test_alert import TestAlertModel

DBT_TEST_ALERT_MOCK = TestAlertModel(
    id="1",
    test_unique_id="1",
    elementary_unique_id="1",
    test_name="test",
    severity="ERROR",
    test_type="dbt_test",
    test_sub_type="sub",
    test_results_description="text",
    test_short_name="tst",
    alert_class_id="1",
    model_unique_id="m1",
)
ELEMENTARY_TEST_ALERT_MOCK = TestAlertModel(
    id="2",
    test_unique_id="2",
    elementary_unique_id="2",
    test_name="test",
    severity="ERROR",
    test_type="elementary_test",
    test_sub_type="sub",
    test_results_description="text",
    test_short_name="tst",
    alert_class_id="2",
    model_unique_id="m1",
)
MODEL_ALERT_MOCK = ModelAlertModel(
    id="m1",
    alias="model",
    path="path/to/model",
    original_path="original/path/to.model",
    materialization="table",
    message="text",
    full_refresh=False,
    alert_class_id="3",
    model_unique_id="m1",
)
SNAPSHOT_ALERT_MOCK = ModelAlertModel(
    id="m2",
    alias="model",
    path="path/to/model",
    original_path="original/path/to.model",
    materialization="snapshot",
    message="text",
    full_refresh=False,
    alert_class_id="4",
    model_unique_id="m2",
)
SOURCE_FRESHNESS_ALERT_MOCK = SourceFreshnessAlertModel(
    id="s1",
    source_name="source",
    identifier="source.s1",
    original_status="error",
    path="path/to/source",
    error="text",
    alert_class_id="5",
    source_freshness_execution_id="s1",
)
GROUPED_NY_TABLE_ALERTS_MOCK = GroupedByTableAlerts(
    alerts=[DBT_TEST_ALERT_MOCK, ELEMENTARY_TEST_ALERT_MOCK, MODEL_ALERT_MOCK]
)


class AlertsDataMock:
    def __init__(self) -> None:
        self.dbt_test = DBT_TEST_ALERT_MOCK
        self.elementary_test = ELEMENTARY_TEST_ALERT_MOCK
        self.model = MODEL_ALERT_MOCK
        self.snapshot = SNAPSHOT_ALERT_MOCK
        self.source_freshness = SOURCE_FRESHNESS_ALERT_MOCK
        self.grouped_by_table = GROUPED_NY_TABLE_ALERTS_MOCK
