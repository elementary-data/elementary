import pytest
from datetime import date, datetime
from unittest import mock
import lineage
from lineage.snowflake_query_history import SnowflakeQueryHistory


@pytest.mark.parametrize("start_date, end_date, database_name, query_type, expected_bindings", [
    (datetime(2025, 9, 11), datetime(2025, 9, 12), 'elementary',
     'account_usage', ('elementary', datetime(2025, 9, 11, 0, 0, 0), datetime(2025, 9, 12, 23, 59, 59))),
    (datetime(2025, 9, 11, 1, 33, 7), datetime(2025, 9, 12, 1, 33, 7), 'elementary',
     'account_usage', ('elementary', datetime(2025, 9, 11, 1, 33, 7), datetime(2025, 9, 12, 1, 33, 7))),
    (datetime(2025, 9, 9), None, 'elementary',
     'account_usage', ('elementary', datetime(2025, 9, 9, 0, 0, 0),)),
    (datetime(2025, 9, 9, 1, 33, 7), None, 'elementary',
     'account_usage', ('elementary', datetime(2025, 9, 9, 1, 33, 7),)),
    (datetime(2025, 9, 12), datetime(2025, 9, 13), 'elementary',
     'information_schema', ('elementary', datetime(2025, 9, 12, 0, 0, 0), datetime(2025, 9, 13, 23, 59, 59))),
    (datetime(2025, 9, 12, 1, 33, 7), datetime(2025, 9, 13, 1, 33, 7), 'elementary',
     'information_schema', ('elementary', datetime(2025, 9, 12, 1, 33, 7), datetime(2025, 9, 13, 1, 33, 7))),
    (datetime(2025, 9, 15, 1, 33, 7), None, 'elementary',
     'information_schema', ('elementary', datetime(2025, 9, 15, 1, 33, 7),)),
    (datetime(2025, 9, 15), None, 'elementary',
     'information_schema', ('elementary', datetime(2025, 9, 15, 0, 0, 0),))
])
@mock.patch(f'{lineage.snowflake_query_history.__name__}.date', wraps=date)
def test_snowflake_query_history_build_history_query(mock_date, start_date, end_date, database_name, query_type,
                                                     expected_bindings):
    mock_date.today.return_value = date(2025, 9, 18)
    query, bindings = SnowflakeQueryHistory._build_history_query(start_date, end_date, database_name)
    assert query_type in query
    assert bindings == expected_bindings
