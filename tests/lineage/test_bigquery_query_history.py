import pytest
from datetime import datetime, timedelta
from google.cloud import bigquery
from lineage.bigquery_query_history import BigQueryQueryHistory


@pytest.mark.parametrize("start_date, end_date, database_name, location", [
    (datetime(2021, 10, 9), datetime(2021, 10, 10), ['db1'], 'us'),
    (datetime(2021, 10, 9), datetime(2021, 10, 10), ['db1', 'db2'], 'us'),
    (datetime(2021, 10, 9), datetime(2021, 10, 10), ['db1', 'db2', 'db3'], 'us'),
    (datetime(2021, 10, 9), None, ['db1'], 'us'),
    (datetime(2021, 10, 9), None, ['db-1', 'db2'], 'US'),
])
def test_bigquery_query_history_build_history_query(start_date, end_date, database_name, location):

    query, params = BigQueryQueryHistory._build_history_query(start_date, end_date, database_name, location)

    if end_date is not None:
        assert BigQueryQueryHistory.INFO_SCHEMA_END_TIME_UP_TO_PARAMETER in query
        assert BigQueryQueryHistory.INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP not in query
        assert bigquery.ScalarQueryParameter("end_time",
                                             "TIMESTAMP",
                                             end_date + timedelta(hours=23, minutes=59, seconds=59)) in params
        assert len(params) == 2
    else:
        assert BigQueryQueryHistory.INFO_SCHEMA_END_TIME_UP_TO_PARAMETER not in query
        assert BigQueryQueryHistory.INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP in query
        assert len(params) == 1
