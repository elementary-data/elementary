from elementary.monitor.data_monitoring.schema import SelectorFilterSchema
from elementary.monitor.fetchers.tests.schema import TestResultSummarySchema
from tests.mocks.fetchers.tests_fetcher_mock import MockTestsFetcher


def test_get_test_results_summary():
    fetcher = MockTestsFetcher()
    test_result_db_rows = fetcher.get_all_test_results_db_rows()

    # No filter
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows
    )
    # No duplicate tests - only latest results
    assert len(test_results_summary) == 4
    for result_summary in test_results_summary:
        assert isinstance(result_summary, TestResultSummarySchema)

    # Tag filter - awesome-o
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows,
        filter=SelectorFilterSchema(tag="awesome-o"),
    )
    assert len(test_results_summary) == 2
    for result_summary in test_results_summary:
        assert isinstance(result_summary, TestResultSummarySchema)
    elementary_unique_ids = [
        result_summary.elementary_unique_id for result_summary in test_results_summary
    ]
    assert elementary_unique_ids == [
        "test_id_1.row_count",
        "test_id_4.generic",
    ]

    # Tag filter - awesome
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows,
        filter=SelectorFilterSchema(tag="awesome"),
    )
    assert len(test_results_summary) == 2
    for result_summary in test_results_summary:
        assert isinstance(result_summary, TestResultSummarySchema)
    elementary_unique_ids = [
        result_summary.elementary_unique_id for result_summary in test_results_summary
    ]
    assert elementary_unique_ids == [
        "test_id_1.row_count",
        "test_id_3.row_count",
    ]

    # Tag filter - no such tag
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows,
        filter=SelectorFilterSchema(tag="no such tag"),
    )
    assert len(test_results_summary) == 0

    # Owner filter - Jeff
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows,
        filter=SelectorFilterSchema(owner="Jeff"),
    )
    assert len(test_results_summary) == 2
    for result_summary in test_results_summary:
        assert isinstance(result_summary, TestResultSummarySchema)
    elementary_unique_ids = [
        result_summary.elementary_unique_id for result_summary in test_results_summary
    ]
    assert elementary_unique_ids == [
        "test_id_1.row_count",
        "test_id_4.generic",
    ]

    # Owner filter - Joe
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows,
        filter=SelectorFilterSchema(owner="Joe"),
    )
    assert len(test_results_summary) == 2
    for result_summary in test_results_summary:
        assert isinstance(result_summary, TestResultSummarySchema)
    elementary_unique_ids = [
        result_summary.elementary_unique_id for result_summary in test_results_summary
    ]
    assert elementary_unique_ids == [
        "test_id_1.row_count",
        "test_id_2.freshness",
    ]

    # Model filter - model_id_1
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows,
        filter=SelectorFilterSchema(model="model_id_1"),
    )
    assert len(test_results_summary) == 2
    for result_summary in test_results_summary:
        assert isinstance(result_summary, TestResultSummarySchema)
    elementary_unique_ids = [
        result_summary.elementary_unique_id for result_summary in test_results_summary
    ]
    assert elementary_unique_ids == [
        "test_id_1.row_count",
        "test_id_4.generic",
    ]

    # Model filter - model_id_2
    test_results_summary = fetcher.get_test_results_summary(
        test_results_db_rows=test_result_db_rows,
        filter=SelectorFilterSchema(model="model_id_2"),
    )
    assert len(test_results_summary) == 1
    for result_summary in test_results_summary:
        assert isinstance(result_summary, TestResultSummarySchema)
    elementary_unique_ids = [
        result_summary.elementary_unique_id for result_summary in test_results_summary
    ]
    assert elementary_unique_ids == ["test_id_2.freshness"]
