import pytest

from tests.mocks.fetchers.tests_fetcher_mock import MockTestsFetcher


def test_get_test_metadata_from_test_result_db_row(
    tests_fetcher_mock: MockTestsFetcher,
):
    test_result_db_rows = tests_fetcher_mock.get_all_test_results_db_rows()
    elementary_test_result_db_row = [
        db_row for db_row in test_result_db_rows if db_row.test_type != "dbt_test"
    ][0]
    dbt_test_result_db_row = [
        db_row for db_row in test_result_db_rows if db_row.test_type == "dbt_test"
    ][0]

    elementary_test_metadata = (
        tests_fetcher_mock.get_test_metadata_from_test_result_db_row(
            elementary_test_result_db_row
        )
    )
    dbt_test_metadata = tests_fetcher_mock.get_test_metadata_from_test_result_db_row(
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
def tests_fetcher_mock() -> MockTestsFetcher:
    return MockTestsFetcher()
