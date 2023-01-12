import json
from pathlib import Path
from typing import Dict

import pytest

_REPORT_DATA_FILENAME = "elementary_output.json"
_REPORT_DATA_FIXTURE = Path(__file__).parent / "fixtures" / _REPORT_DATA_FILENAME
_REPORT_DATA_PATH = Path(_REPORT_DATA_FILENAME)

TotalsEntry = Dict[str, int]
Totals = Dict[str, TotalsEntry]


def test_report_keys(report_data_fixture):
    report_data = get_report_data()
    assert report_data.keys() == report_data_fixture.keys()


def test_totals(report_data_fixture):
    report_data = get_report_data()
    for key in report_data_fixture:
        if key.endswith("_totals"):
            assert_totals(report_data[key], report_data_fixture[key])


def test_sidebar(report_data_fixture):
    report_data = get_report_data()
    assert (
        "model.elementary_integration_tests.error_model"
        in report_data["sidebars"]["dbt"]["elementary_integration_tests"]["models"][
            "__files__"
        ]
    )
    assert (
        "model.elementary_integration_tests.nested"
        in report_data["sidebars"]["dbt"]["elementary_integration_tests"]["models"][
            "nested"
        ]["models"]["tree"]["__files__"]
    )
    assert (
        "source.elementary_integration_tests.training.any_type_column_anomalies_training"
        in report_data["sidebars"]["dbt"]["elementary_integration_tests"]["sources"][
            "__files__"
        ]
    )
    assert (
        "model.elementary_integration_tests.any_type_column_anomalies"
        in report_data["sidebars"]["owners"]["@edr"]
    )
    assert (
        "model.elementary_integration_tests.any_type_column_anomalies"
        not in report_data["sidebars"]["owners"]["No owners"]
    )
    assert (
        "model.elementary_integration_tests.string_column_anomalies"
        in report_data["sidebars"]["tags"]["marketing"]
    )
    assert (
        "model.elementary_integration_tests.string_column_anomalies"
        not in report_data["sidebars"]["tags"]["No tags"]
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
