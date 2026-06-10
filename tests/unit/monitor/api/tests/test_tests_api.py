import pytest

from elementary.monitor.fetchers.tests.schema import TestResultDBRowSchema
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


# ---------------------------------------------------------------------------
# _normalize_results_sample
# ---------------------------------------------------------------------------


def test_normalize_results_sample_with_list():
    """List-shaped sample_data passes through unchanged."""
    rows = [{"id": 1}, {"id": 2}]
    result = MockTestsAPI._normalize_results_sample(rows)
    assert result == rows


def test_normalize_results_sample_with_dict_rows_key():
    """Dict payload with 'rows' key is unwrapped correctly (issue #2269)."""
    rows = [{"id": 10}, {"id": 11}]
    result = MockTestsAPI._normalize_results_sample({"rows": rows})
    assert result == rows


@pytest.mark.parametrize("key", ["sample_rows", "results_sample", "data"])
def test_normalize_results_sample_with_dict_other_keys(key):
    """Other common dict wrapper keys are also unwrapped."""
    rows = [{"id": 1}]
    result = MockTestsAPI._normalize_results_sample({key: rows})
    assert result == rows


def test_normalize_results_sample_with_none():
    assert MockTestsAPI._normalize_results_sample(None) is None


def test_normalize_results_sample_with_unknown_dict():
    """Dict with no recognized key returns None rather than crashing."""
    result = MockTestsAPI._normalize_results_sample({"unknown_key": [{"id": 1}]})
    assert result is None


# ---------------------------------------------------------------------------
# _get_test_result_from_test_result_db_row – results_sample population
# ---------------------------------------------------------------------------


def _make_dbt_row(**overrides) -> TestResultDBRowSchema:
    defaults = dict(
        id="r1",
        model_unique_id="model_id_1",
        test_unique_id="test_id_x",
        elementary_unique_id="test_id_x.generic",
        detected_at="2023-01-01 10:00:00",
        database_name="db",
        schema_name="schema",
        table_name="table",
        column_name="col",
        test_type="dbt_test",
        test_sub_type="generic",
        test_results_description="Got 2 results, configured to fail if != 0",
        test_description=None,
        original_path="models/test.sql",
        owners="[]",
        model_owner="[]",
        tags="[]",
        test_tags="[]",
        model_tags="[]",
        meta="{}",
        model_meta="{}",
        test_results_query=None,
        other=None,
        test_name="my_test",
        test_params="{}",
        severity="ERROR",
        status="fail",
        test_created_at="2023-01-01 09:00:00",
        days_diff=1,
        invocations_rank_index=1,
        sample_data=None,
        failures=2,
        package_name=None,
        execution_time=None,
        invocation_id=None,
        test_execution_id=None,
    )
    defaults.update(overrides)
    return TestResultDBRowSchema(**defaults)


def test_results_sample_populated_from_list_sample_data():
    """List-shaped sample_data → results_sample is populated (existing behaviour)."""
    rows = [{"col": "bad_val_1"}, {"col": "bad_val_2"}]
    row = _make_dbt_row(sample_data=rows)
    result = MockTestsAPI._get_test_result_from_test_result_db_row(row)
    assert result is not None
    assert result.results_sample == rows


def test_results_sample_populated_from_dict_sample_data():
    """Dict-shaped sample_data → results_sample is still populated (fix for #2269)."""
    rows = [{"col": "bad_val_1"}, {"col": "bad_val_2"}]
    row = _make_dbt_row(sample_data={"rows": rows})
    result = MockTestsAPI._get_test_result_from_test_result_db_row(row)
    assert result is not None
    assert result.results_sample == rows


def test_results_sample_empty_when_none():
    row = _make_dbt_row(sample_data=None)
    result = MockTestsAPI._get_test_result_from_test_result_db_row(row)
    assert result is not None
    assert result.results_sample is None


def test_results_sample_suppressed_when_disable_samples():
    rows = [{"col": "bad_val"}]
    row = _make_dbt_row(sample_data=rows)
    result = MockTestsAPI._get_test_result_from_test_result_db_row(row, disable_samples=True)
    assert result is not None
    assert result.results_sample is None


def test_results_sample_suppressed_for_dict_sample_data_when_disable_samples():
    """disable_samples=True must suppress rows even when sample_data is dict-shaped."""
    rows = [{"col": "bad_val"}]
    row = _make_dbt_row(sample_data={"rows": rows})
    result = MockTestsAPI._get_test_result_from_test_result_db_row(row, disable_samples=True)
    assert result is not None
    assert result.results_sample is None


# ---------------------------------------------------------------------------
# Integration: get_test_results includes sample rows from dict-shaped payload
# ---------------------------------------------------------------------------


def test_get_test_results_includes_dict_sample_rows(tests_api_mock: MockTestsAPI):
    """End-to-end: dict-shaped sample_data in the fetcher mock reaches the report payload."""
    test_results = tests_api_mock.get_test_results(invocation_id=None)

    # Flatten all result objects from all model keys
    all_results = [r for results in test_results.values() for r in results]

    # Find the two new mock rows added for issue #2269
    list_sample_result = next(
        (r for r in all_results if r.metadata.test_unique_id == "test_id_5"), None
    )
    dict_sample_result = next(
        (r for r in all_results if r.metadata.test_unique_id == "test_id_6"), None
    )

    assert list_sample_result is not None, "mock row test_id_5 (list sample) not found"
    assert dict_sample_result is not None, "mock row test_id_6 (dict sample) not found"

    assert list_sample_result.test_results.results_sample == [
        {"id": 1, "val": "a"},
        {"id": 2, "val": "b"},
        {"id": 3, "val": "c"},
    ]
    assert dict_sample_result.test_results.results_sample == [
        {"id": 10, "val": "x"},
        {"id": 11, "val": "y"},
    ]
