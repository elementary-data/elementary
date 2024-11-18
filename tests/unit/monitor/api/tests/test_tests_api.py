import pytest

from tests.mocks.api.tests_api_mock import MockTestsAPI


def test_get_test_metadata_from_test_result_db_row(
    tests_api_mock: MockTestsAPI,
):
    test_result_db_rows = tests_api_mock.tests_fetcher.get_all_test_results_db_rows()
    elementary_test_result_db_row = test_result_db_rows[0]
    dbt_test_result_db_row = test_result_db_rows[-1]

    elementary_test_metadata = (
        tests_api_mock._get_test_metadata_from_test_result_db_row(
            elementary_test_result_db_row
        )
    )
    dbt_test_metadata = tests_api_mock._get_test_metadata_from_test_result_db_row(
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


def test_parse_test_db_row(
    tests_api_mock: MockTestsAPI,
):
    tests = tests_api_mock.tests_fetcher.get_tests()
    test = tests_api_mock._parse_test_db_row(tests[0])
    assert test.table_unique_id == "test_db.test_schema.table"
    assert test.configuration.get("test_name") == test.name
    assert test.configuration == {
        "anomaly_threshold": 3,
        "test_name": "the_test_1",
        "testing_timeframe": "1 hour",
        "timestamp_column": "signup_date",
    }
    assert test.display_name == "The Test 1"
    assert set(test.tags) == {"awesome", "awesome-o"}
    assert (
        test.normalized_full_path
        == "elementary/tests/elementary/tests/test_elementary.py"
    )


@pytest.fixture
def tests_api_mock() -> MockTestsAPI:
    return MockTestsAPI()
