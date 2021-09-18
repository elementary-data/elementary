import pytest
from datetime import datetime
from lineage.query_history import QueryHistory


@pytest.mark.parametrize("end_date, expected_result", [
    (datetime(2021, 9, 9), datetime(2021, 9, 9, 23, 59, 59)),
    (datetime(2021, 9, 9, 11, 50, 3), datetime(2021, 9, 9, 11, 50, 3)),
    (datetime(2021, 9, 9, 0, 0, 3), datetime(2021, 9, 9, 0, 0, 3)),
    (datetime(2021, 9, 9, 0, 0, 0), datetime(2021, 9, 9, 23, 59, 59)),
    (None, None),
])
def test_query_history_include_end_date(end_date, expected_result):
    assert QueryHistory._include_end_date(end_date) == expected_result
