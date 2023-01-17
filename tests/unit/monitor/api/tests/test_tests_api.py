import pytest

from elementary.monitor.api.tests.schema import TestResultDBRowSchema
from tests.mocks.api.tests_api_mock import MockTestsAPI


def test_get_test_metadata_from_test_result_db_row(tests_api_mock: MockTestsAPI):
    elementary_test_result_db_row = TestResultDBRowSchema(
        id="mock_id_1",
        model_unique_id="model_id_1",
        test_unique_id="test_id_1",
        elementary_unique_id="test_id_1.row_count",
        detected_at="2023-01-01 10:00:00",
        database_name="test_db",
        schema_name="test_schema",
        table_name="table",
        column_name=None,
        test_type="anomaly_detection",
        test_sub_type="row_count",
        test_results_description="This is a fine result",
        owners='["Jeff", "Joe"]',
        tags='["awesome", "awesome-o"]',
        meta='{ "subscribers": ["@jeff", "joe"], "alert_fields": ["table", "column", "description"] }',
        test_results_query="select * from table",
        other=None,
        test_name="The test 1",
        test_params='{"table_anomalies": ["row_count"], "time_bucket": {"period": "hour", "count": 1}, "model": "{{ get_where_subquery(ref(\'customers\')) }}", "sensitivity": 3, "timestamp_column": "signup_date", "backfill_days": 2}',
        severity="ERROR",
        status="fail",
        test_created_at="2023-01-01 09:00:00",
        days_diff=1,
        invocations_rank_index=1,
    )
    dbt_test_result_db_row = TestResultDBRowSchema(
        id="mock_id_2",
        model_unique_id="model_id_1",
        test_unique_id="test_id_2",
        elementary_unique_id="test_id_2.generic",
        detected_at="2023-01-01 10:00:00",
        database_name="test_db",
        schema_name="test_schema",
        table_name="table",
        column_name="column",
        test_type="dbt_test",
        test_sub_type="generic",
        test_results_description="This is a fine result",
        owners='["Jeff", "Joe"]',
        tags='["awesome", "awesome-o"]',
        meta="{}",
        test_results_query="select * from table",
        other=None,
        test_name="The test 2",
        test_params='{"column_name": "missing_column", "model": "{{ get_where_subquery(ref(\'error_model\')) }}"}',
        severity="ERROR",
        status="fail",
        test_created_at="2023-01-01 09:00:00",
        days_diff=1,
        invocations_rank_index=1,
    )

    elementary_test_metadata = tests_api_mock.get_test_metadata_from_test_result_db_row(
        elementary_test_result_db_row
    )
    dbt_test_metadata = tests_api_mock.get_test_metadata_from_test_result_db_row(
        dbt_test_result_db_row
    )

    # Test elementary test configuration generated correctly
    assert (
        elementary_test_metadata.configuration.get("test_name")
        == elementary_test_result_db_row.test_name
    )
    assert elementary_test_metadata.configuration.get(
        "timestamp_column"
    ) == elementary_test_result_db_row.test_params.get("timestamp_column")
    assert elementary_test_metadata.configuration.get("testing_timeframe") == "1 hour"
    assert elementary_test_metadata.configuration.get(
        "anomaly_threshold"
    ) == elementary_test_result_db_row.test_params.get("sensitivity")

    # Test dbt test configuration generated correctly
    assert (
        dbt_test_metadata.configuration.get("test_name")
        == dbt_test_result_db_row.test_name
    )
    assert (
        dbt_test_metadata.configuration.get("test_params")
        == dbt_test_result_db_row.test_params
    )


@pytest.fixture
def tests_api_mock() -> MockTestsAPI:
    return MockTestsAPI()
