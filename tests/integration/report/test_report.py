import json
from pathlib import Path
from typing import Dict

import pytest

_REPORT_DATA_FILENAME = "elementary_output.json"
_REPORT_DATA_FIXTURE = Path(__file__).parent / "fixtures" / _REPORT_DATA_FILENAME
_REPORT_DATA_PATH = Path(_REPORT_DATA_FILENAME)

TotalsEntry = Dict[str, int]
Totals = Dict[str, TotalsEntry]

report_data = json.loads(_REPORT_DATA_PATH.read_text())


def test_report_keys(report_data_fixture):
    assert report_data.keys() == report_data_fixture.keys()


def test_totals(report_data_fixture):
    for key in report_data_fixture:
        if key.endswith("_totals"):
            assert_totals(report_data[key], report_data_fixture[key])


def test_models(report_data_fixture):
    assert report_data["models"] == report_data_fixture["models"]


def test_lineage(report_data_fixture):
    assert report_data["lineage"] == report_data_fixture["lineage"]


def test_coverage(report_data_fixture):
    assert report_data["coverages"] == report_data_fixture["coverages"]


def assert_totals(data_totals: Totals, fixture_totals: Totals):
    for total_key in fixture_totals:
        assert_totals_entry(data_totals[total_key], fixture_totals[total_key])


def assert_totals_entry(
    data_total_entries: TotalsEntry, fixture_total_entries: TotalsEntry
):
    for key in fixture_total_entries:
        assert data_total_entries[key] * fixture_total_entries[key] >= 0


@pytest.fixture
def report_data_fixture():
    return json.loads(_REPORT_DATA_FIXTURE.read_text())
