from elementary.monitor.api.tests.utils import (
    get_display_name,
    get_normalized_full_path,
    get_table_full_name,
    get_test_configuration,
)


def test_get_table_full_name():
    assert get_table_full_name("db", "schema", "table") == "db.schema.table"
    assert get_table_full_name(None, "schema", "table") == "schema.table"
    assert get_table_full_name("db", None, "table") == "db.table"
    assert get_table_full_name("db", "schema", None) == ""


def test_get_display_name():
    assert get_display_name("test_name") == "Test Name"
    assert get_display_name("test_name_1") == "Test Name 1"
    assert get_display_name("TEST_NAME") == "Test Name"


def test_get_test_configuration():
    assert get_test_configuration(None, "test_name", {}) == {}
    assert get_test_configuration("dbt_test", "test_name", {}) == {
        "test_name": "test_name",
        "test_params": {},
    }
    assert get_test_configuration(
        "test", "test_name", {"time_bucket": {"count": 1, "period": "day"}}
    ) == {
        "test_name": "test_name",
        "timestamp_column": None,
        "testing_timeframe": "1 day",
        "anomaly_threshold": None,
    }
    assert get_test_configuration(
        "test",
        "test_name",
        {"time_bucket": {"count": 2, "period": "hour"}, "sensitivity": 3},
    ) == {
        "test_name": "test_name",
        "timestamp_column": None,
        "testing_timeframe": "2 hours",
        "anomaly_threshold": 3,
    }
    assert get_test_configuration(
        "test",
        "test_name",
        {"time_bucket": {"count": 2, "period": "hour"}, "anomaly_sensitivity": 3},
    ) == {
        "test_name": "test_name",
        "timestamp_column": None,
        "testing_timeframe": "2 hours",
        "anomaly_threshold": 3,
    }
    assert get_test_configuration(
        "test",
        "test_name",
        {
            "timestamp_column": "signup_date",
            "time_bucket": {"count": 1, "period": "day"},
        },
    ) == {
        "test_name": "test_name",
        "timestamp_column": "signup_date",
        "testing_timeframe": "1 day",
        "anomaly_threshold": None,
    }


def test_get_normalized_full_path():
    assert get_normalized_full_path("package", "path") == "package/path"
    assert get_normalized_full_path(None, "path") == "path"
    assert get_normalized_full_path("package", None) is None
    assert get_normalized_full_path(None, None) is None
