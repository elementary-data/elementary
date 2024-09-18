import json
from pathlib import Path
from typing import Dict

import pytest

_REPORT_DATA_FILENAME = "elementary_output.json"
_REPORT_DATA_FIXTURE = Path(__file__).parent / "fixtures" / _REPORT_DATA_FILENAME
_REPORT_DATA_PATH = Path("edr_target/" + _REPORT_DATA_FILENAME)

TotalsEntry = Dict[str, int]
Totals = Dict[str, TotalsEntry]


def test_report_keys(report_data_fixture):
    report_data = get_report_data()
    assert sorted(list(report_data.keys())) == sorted(list(report_data_fixture.keys()))


def test_group():  # deprecated
    report_data = get_report_data()
    assert {
        "node_id": "model.elementary_integration_tests.error_model",
        "resource_type": "model",
    } in report_data["groups"]["dbt"]["elementary_integration_tests"]["models"][
        "__files__"
    ]
    assert {
        "node_id": "model.elementary_integration_tests.nested",
        "resource_type": "model",
    } in report_data["groups"]["dbt"]["elementary_integration_tests"]["models"][
        "nested"
    ][
        "models"
    ][
        "tree"
    ][
        "__files__"
    ]
    assert {
        "node_id": "source.elementary_integration_tests.training.any_type_column_anomalies_training",
        "resource_type": "source",
    } in report_data["groups"]["dbt"]["elementary_integration_tests"]["sources"][
        "__files__"
    ]
    assert {
        "node_id": "model.elementary_integration_tests.any_type_column_anomalies",
        "resource_type": "model",
    } in report_data["groups"]["owners"]["@edr"]
    assert {
        "node_id": "model.elementary_integration_tests.any_type_column_anomalies",
        "resource_type": "model",
    } not in report_data["groups"]["owners"]["No owners"]
    assert {
        "node_id": "model.elementary_integration_tests.string_column_anomalies",
        "resource_type": "model",
    } in report_data["groups"]["tags"]["marketing"]
    assert {
        "node_id": "model.elementary_integration_tests.string_column_anomalies",
        "resource_type": "model",
    } not in report_data["groups"]["tags"]["No tags"]


def test_group_views():
    report_data = get_report_data()
    groups = report_data.get("groups")

    assert "bi_assets" in groups
    assert groups["bi_assets"] is None

    assert "data_assets" in groups
    data_assets_group = groups["data_assets"]
    assert len(data_assets_group) == 4
    assert data_assets_group[0]["name"] == "dwh"
    dwh_view = data_assets_group[0]["data"]
    assert data_assets_group[1]["name"] == "dbt"
    dbt_view = data_assets_group[1]["data"]
    assert data_assets_group[2]["name"] == "tags"
    tags_view = data_assets_group[2]["data"]
    assert data_assets_group[3]["name"] == "owners"
    owners_view = data_assets_group[3]["data"]

    assert "postgres" in dwh_view
    assert {
        "node_id": "model.elementary_integration_tests.error_model",
        "resource_type": "model",
    } in dbt_view["elementary_integration_tests"]["models"]["__files__"]
    assert {
        "node_id": "model.elementary_integration_tests.nested",
        "resource_type": "model",
    } in dbt_view["elementary_integration_tests"]["models"]["nested"]["models"]["tree"][
        "__files__"
    ]
    assert {
        "node_id": "source.elementary_integration_tests.training.any_type_column_anomalies_training",
        "resource_type": "source",
    } in dbt_view["elementary_integration_tests"]["sources"]["__files__"]
    assert {
        "node_id": "model.elementary_integration_tests.any_type_column_anomalies",
        "resource_type": "model",
    } in owners_view["@edr"]
    assert {
        "node_id": "model.elementary_integration_tests.any_type_column_anomalies",
        "resource_type": "model",
    } not in owners_view["No owners"]
    assert {
        "node_id": "model.elementary_integration_tests.string_column_anomalies",
        "resource_type": "model",
    } in tags_view["marketing"]
    assert {
        "node_id": "model.elementary_integration_tests.string_column_anomalies",
        "resource_type": "model",
    } not in tags_view["No tags"]


def test_duplicate_test_runs():
    report_data = get_report_data()
    test_runs = report_data.get("test_runs")
    for tests in test_runs.values():
        for test in tests:
            runs = test.get("test_runs")
            invocations = runs.get("invocations")
            invocation_ids = [invocation.get("id") for invocation in invocations]
            assert len(invocation_ids) == len(set(invocation_ids))


def test_test_runs_are_sorted():
    report_data = get_report_data()
    test_runs = report_data.get("test_runs")
    for tests in test_runs.values():
        for test in tests:
            runs = test.get("test_runs")
            invocations = runs.get("invocations")
            invocation_times = [
                invocation.get("time_utc") for invocation in invocations
            ]
            sorted_invocation_times = [*invocation_times]
            sorted_invocation_times.sort()
            assert invocation_times == sorted_invocation_times


def test_report_tests():
    report_data = get_report_data()
    tests = report_data.get("tests")
    test_results = report_data.get("test_results")

    assert_test_with_results_included_in_the_tests(tests, test_results)
    assert_test_without_results_do_not_have_results_dependent_fields(
        tests, test_results
    )


def _get_test_unique_ids_with_results(test_results):
    return [
        result["metadata"]["test_unique_id"]
        for results in test_results.values()
        for result in results
    ]


def assert_test_with_results_included_in_the_tests(tests, test_results):
    test_unique_ids_with_results = _get_test_unique_ids_with_results(test_results)
    assert all(
        test_unique_id in tests for test_unique_id in test_unique_ids_with_results
    )


def assert_test_without_results_do_not_have_results_dependent_fields(
    tests, test_results
):
    test_unique_ids_with_results = _get_test_unique_ids_with_results(test_results)
    test_unique_ids_with_no_results = [
        test_unique_id
        for test_unique_id in tests
        if test_unique_id not in test_unique_ids_with_results
    ]
    assert all(
        tests[test_unique_id]["test_type"] is None
        for test_unique_id in test_unique_ids_with_no_results
    )
    assert all(
        tests[test_unique_id]["test_sub_type"] is None
        for test_unique_id in test_unique_ids_with_no_results
    )
    assert all(
        tests[test_unique_id]["created_at"] is None
        for test_unique_id in test_unique_ids_with_no_results
    )
    assert all(
        tests[test_unique_id]["latest_run_time"] is None
        for test_unique_id in test_unique_ids_with_no_results
    )
    assert all(
        tests[test_unique_id]["latest_run_status"] is None
        for test_unique_id in test_unique_ids_with_no_results
    )


# This test currently uses fixed data points that are unmaintainable upon adding tests.
# def test_duplicate_rows_for_latest_run_status(warehouse_type):
#     report_data = get_report_data()
#     # e2e contains 2 failed dimension tests and 1 passed
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="dimension_anomalies",
#         status="fail",
#         expected_amount=2,
#     )
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="dimension_anomalies",
#         status="pass",
#         expected_amount=1,
#     )
#
#     # Currently schema changes tests are not supported at databricks
#     if warehouse_type != "databricks":
#         # e2e contains 1 passed schema change test for groups table
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="groups",
#         )
#
#         # e2e contains 3 failed schema change test for stats_player table
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="stats_players",
#             expected_amount=3,
#         )
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="stats_players",
#             column="offsides",
#             expected_amount=1,
#         )
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="stats_players",
#             column="red_cards",
#             expected_amount=1,
#         )
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="stats_players",
#             column="key_crosses",
#             expected_amount=1,
#         )
#
#         # e2e contains 1 passed schema change test for stats_team table
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="stats_team",
#         )
#
#         # e2e contains 1 passed schema change test for string_column_anomalies table
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="string_column_anomalies",
#         )
#
#         # e2e contains 1 passed schema change test for numeric_column_anomalies table
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes",
#             table="numeric_column_anomalies",
#         )
#
#         # e2e contains 2 failed and 1 error schema changes from baseline tests for groups table
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes_from_baseline",
#             table="groups",
#             status="fail",
#             expected_amount=2,
#         )
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes_from_baseline",
#             table="groups",
#             status="error",
#             expected_amount=1,
#         )
#
#         # e2e contains 2 schema changes from baseline tests for goals column at stats_players table (2 different tests)
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes_from_baseline",
#             table="stats_players",
#             column="goals",
#             expected_amount=2,
#         )
#
#         # e2e contains 2 schema changes from baseline tests for coffee_cups_consumed column at stats_players table (2 different tests)
#         assert_test_counter(
#             report_data=report_data,
#             test_type="schema_change",
#             name="schema_changes_from_baseline",
#             table="stats_players",
#             column="coffee_cups_consumed",
#             expected_amount=2,
#         )
#
#     # e2e contains only 4 singular failed tests
#     assert_test_counter(
#         report_data=report_data,
#         test_type="dbt_test",
#         test_sub_type="singular",
#         status="fail",
#         expected_amount=4,
#     )
#     assert_test_counter(
#         report_data=report_data,
#         test_type="dbt_test",
#         test_sub_type="singular",
#         expected_amount=4,
#     )
#
#     #     # e2e contains 58 all columns anomalies tests
#     #     assert_test_counter(
#     #         report_data=report_data,
#     #         test_type="anomaly_detection",
#     #         name="all_columns_anomalies",
#     #         expected_amount=58,
#     #     )
#
#     #     # All of the tests are defined on the table any_type_column_anomalies
#     #     assert_test_counter(
#     #         report_data=report_data,
#     #         test_type="anomaly_detection",
#     #         name="all_columns_anomalies",
#     #         table="any_type_column_anomalies",
#     #         expected_amount=58,
#     #     )
#
#     # occurred_at column should have 2 passed test results.
#     # 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="occurred_at",
#         status="pass",
#         expected_amount=2,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="occurred_at",
#         test_sub_types=["null_count", "null_percent"],
#     )
#
#     # updated_at column should have 2 passed test results.
#     # 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="updated_at",
#         status="pass",
#         expected_amount=2,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="updated_at",
#         test_sub_types=["null_count", "null_percent"],
#     )
#
#     # null_percent_str column should have 7 test results.
#     # 5 for string type column, 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_str",
#         expected_amount=7,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_str",
#         test_sub_types=[
#             "null_count",
#             "null_percent",
#             "min_length",
#             "max_length",
#             "average_length",
#             "missing_count",
#             "missing_percent",
#         ],
#     )
#
#     # null_percent_int column should have 9 test results.
#     # 7 for numeric type column, 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_int",
#         expected_amount=9,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_int",
#         test_sub_types=[
#             "null_count",
#             "null_percent",
#             "min",
#             "max",
#             "zero_count",
#             "zero_percent",
#             "standard_deviation",
#             "variance",
#             "average",
#         ],
#     )
#
#     # null_percent_float column should have 9 test results.
#     # 7 for numeric type column, 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_float",
#         expected_amount=9,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_float",
#         test_sub_types=[
#             "null_count",
#             "null_percent",
#             "min",
#             "max",
#             "zero_count",
#             "zero_percent",
#             "standard_deviation",
#             "variance",
#             "average",
#         ],
#     )
#
#     # null_percent_bool column should have 2 passed test results.
#     # 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_bool",
#         expected_amount=2,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_percent_bool",
#         test_sub_types=["null_count", "null_percent"],
#     )
#
#     # null_count_str column should have 7 test results.
#     # 5 for string type column, 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_str",
#         expected_amount=7,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_str",
#         test_sub_types=[
#             "null_count",
#             "null_percent",
#             "min_length",
#             "max_length",
#             "average_length",
#             "missing_count",
#             "missing_percent",
#         ],
#     )
#
#     # null_count_int column should have 9 test results.
#     # 7 for numeric type column, 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_int",
#         expected_amount=9,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_int",
#         test_sub_types=[
#             "null_count",
#             "null_percent",
#             "min",
#             "max",
#             "zero_count",
#             "zero_percent",
#             "standard_deviation",
#             "variance",
#             "average",
#         ],
#     )
#
#     # null_count_float column should have 9 test results.
#     # 7 for numeric type column, 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_float",
#         expected_amount=9,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_float",
#         test_sub_types=[
#             "null_count",
#             "null_percent",
#             "min",
#             "max",
#             "zero_count",
#             "zero_percent",
#             "standard_deviation",
#             "variance",
#             "average",
#         ],
#     )
#
#     # null_count_bool column should have 2 passed test results.
#     # 2 for any type column
#     assert_test_counter(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_bool",
#         expected_amount=2,
#     )
#     assert_test_sub_types_occurre_only_once(
#         report_data=report_data,
#         test_type="anomaly_detection",
#         name="all_columns_anomalies",
#         table="any_type_column_anomalies",
#         column="null_count_bool",
#         test_sub_types=["null_count", "null_percent"],
#     )


def assert_test_counter(
    report_data,
    test_type,
    table=None,
    column=None,
    test_sub_type=None,
    status=None,
    name=None,
    expected_amount=1,
):
    test_results = report_data.get("test_results")
    tests_found = 0
    for tests in test_results.values():
        for test in tests:
            test_metadata = test.get("metadata")
            match_test_type = test_metadata.get("test_type") == test_type
            match_table = (
                test_metadata.get("table_name", "").lower() == table.lower()
                if table and test_metadata.get("table_name")
                else True
            )
            match_column = (
                test_metadata.get("column_name").lower() == column.lower()
                if column and test_metadata.get("column_name")
                else True
            )
            match_test_sub_type = (
                test_metadata.get("test_sub_type") == test_sub_type
                if test_sub_type
                else True
            )
            match_status = (
                test_metadata.get("latest_run_status") == status if status else True
            )
            match_name = test_metadata.get("test_name") == name if name else True
            if (
                match_test_type
                and match_table
                and match_column
                and match_test_sub_type
                and match_status
                and match_name
            ):
                tests_found += 1
    assert tests_found == expected_amount


def assert_test_sub_types_occurre_only_once(
    report_data, test_type, test_sub_types, table=None, column=None, name=None
):
    for sub_type in test_sub_types:
        assert_test_counter(
            report_data=report_data,
            test_sub_type=sub_type,
            test_type=test_type,
            table=table,
            column=column,
            name=name,
            expected_amount=1,
        )


def assert_totals(data_totals: Totals, fixture_totals: Totals):
    for total_key in data_totals:
        assert_totals_entry(data_totals[total_key], fixture_totals[total_key])


def assert_totals_entry(
    data_total_entries: TotalsEntry, fixture_total_entries: TotalsEntry
):
    for key in data_total_entries:
        assert data_total_entries[key] * fixture_total_entries[key] >= 0


@pytest.fixture
def report_data_fixture():
    return json.loads(_REPORT_DATA_FIXTURE.read_text())


def get_report_data():
    return json.loads(_REPORT_DATA_PATH.read_text())


# A fixture that returns the value of "--warehouse-type" pytest cli arg.
@pytest.fixture(scope="session")
def warehouse_type(pytestconfig):
    return pytestconfig.getoption("warehouse_type")
