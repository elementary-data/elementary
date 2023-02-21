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


@pytest.fixture
def tests_api_mock() -> MockTestsAPI:
    return MockTestsAPI()
