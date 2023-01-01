import json
from pathlib import Path

import pytest

_REPORT_DATA_FILENAME = "elementary_output.json"
_REPORT_DATA_FIXTURE = Path(__file__).parent / "fixtures" / _REPORT_DATA_FILENAME
_REPORT_DATA_PATH = Path(_REPORT_DATA_FILENAME)


@pytest.fixture
def report_data():
    return json.loads(_REPORT_DATA_PATH.read_text())


@pytest.fixture
def report_data_fixture():
    return json.loads(_REPORT_DATA_FIXTURE.read_text())


def test_totals(report_data, report_data_fixture):
    for key in report_data:
        if key.endswith("_totals") or key in ["coverages", "lineage"]:
            assert report_data[key] == report_data_fixture[key]
