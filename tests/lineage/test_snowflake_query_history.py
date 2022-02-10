import pytest
from datetime import date, datetime
from unittest import mock
import lineage
from lineage.snowflake_query_history import SnowflakeQueryHistory


def expected_params(start_date, end_date=None, dbs=None, dbs_in_like_statement=None):

    params = {'start_date': start_date}

    if end_date is not None:
        params['end_date'] = end_date
    if dbs is not None:
        params['database_names'] = dbs
    if dbs_in_like_statement is not None:
        params['database_names_in_like_statement'] = dbs_in_like_statement

    return params


@pytest.mark.parametrize("start_date, end_date, dbs, query_type, expected_bindings", [
    (datetime(2025, 9, 11), datetime(2025, 9, 12), ['db'],
     'account_usage', expected_params(datetime(2025, 9, 11, 0, 0, 0),
                                      datetime(2025, 9, 12, 23, 59, 59),
                                      ['db'],
                                      ['%db%'])),
    (datetime(2025, 9, 11, 1, 33, 7), datetime(2025, 9, 12, 1, 33, 7), ['DB', 'DB1', 'db2'],
     'account_usage', expected_params(datetime(2025, 9, 11, 1, 33, 7),
                                      datetime(2025, 9, 12, 1, 33, 7),
                                      ['db', 'db1', 'db2'],
                                      ['%db%', '%db1%', '%db2%'])),
    (datetime(2025, 9, 9), None, ['db'],
     'account_usage', expected_params(datetime(2025, 9, 9, 0, 0, 0), None, ['db'], ['%db%'])),
    (datetime(2025, 9, 9, 1, 33, 7), None, ['db'],
     'account_usage', expected_params(datetime(2025, 9, 9, 1, 33, 7), None, ['db'], ['%db%'])),
    (datetime(2025, 9, 12), datetime(2025, 9, 13), ['db'],
     'information_schema', expected_params(datetime(2025, 9, 12, 0, 0, 0), datetime(2025, 9, 13, 23, 59, 59))),
    (datetime(2025, 9, 12, 1, 33, 7), datetime(2025, 9, 13, 1, 33, 7), ['db'],
     'information_schema', expected_params(datetime(2025, 9, 12, 1, 33, 7), datetime(2025, 9, 13, 1, 33, 7))),
    (datetime(2025, 9, 12, 1, 33, 7), datetime(2025, 9, 13, 1, 33, 7), ['db', 'db1', 'db2'],
     'information_schema', expected_params(datetime(2025, 9, 12, 1, 33, 7), datetime(2025, 9, 13, 1, 33, 7))),
    (datetime(2025, 9, 15, 1, 33, 7), None, ['db'],
     'information_schema', expected_params(datetime(2025, 9, 15, 1, 33, 7))),
    (datetime(2025, 9, 15), None, ['db', 'db1'],
     'information_schema', expected_params(datetime(2025, 9, 15, 0, 0, 0)))
])
@mock.patch(f'{lineage.snowflake_query_history.__name__}.date', wraps=date)
def test_snowflake_query_history_build_history_query(mock_date, start_date, end_date, dbs, query_type,
                                                     expected_bindings):
    mock_date.today.return_value = date(2025, 9, 18)
    if query_type == 'account_usage':
        query, bindings = SnowflakeQueryHistory._account_usage_query_history(start_date, end_date, dbs)
    else:
        query, bindings = SnowflakeQueryHistory._info_schema_query_history(start_date, end_date, dbs)
    assert query_type in query
    assert bindings == expected_bindings
