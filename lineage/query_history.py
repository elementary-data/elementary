from datetime import datetime, timedelta
from typing import Optional
from lineage.bigquery_query import BigQueryQuery
from lineage.exceptions import SerializationError
from lineage.query import Query
from lineage.query_history_stats import QueryHistoryStats
from lineage.snowflake_query import SnowflakeQuery
from lineage.utils import is_flight_mode_on
import json
import os


class QueryHistory(object):

    INFORMATION_SCHEMA_QUERY_HISTORY = None

    LATEST_QUERY_HISTORY_FILE = './latest_query_history.json'

    def __init__(self, con, profile_database_name: str, profile_schema_name: str,
                 should_export_query_history: bool = True, ignore_schema: bool = False) -> None:
        self._con = con
        self._profile_database_name = profile_database_name
        self._profile_schema_name = profile_schema_name
        self._should_export_query_history = should_export_query_history
        self._ignore_schema = ignore_schema
        self._query_history_stats = QueryHistoryStats()

    def _serialize_query_history(self, queries: [str]) -> None:
        if self._should_export_query_history:
            with open(self.LATEST_QUERY_HISTORY_FILE, 'w') as query_history_file:
                serialized_queries = []
                for query in queries:
                    serialized_queries.append(query.to_dict())
                json.dump(serialized_queries, query_history_file)

    def _deserialize_query_history(self) -> [Query]:
        deserialized_queries = []
        if os.path.exists(self.LATEST_QUERY_HISTORY_FILE):
            with open(self.LATEST_QUERY_HISTORY_FILE, 'r') as query_history_file:
                queries = json.load(query_history_file)
                for query_dict in queries:
                    platform_type = query_dict.pop('platform_type')
                    if platform_type == SnowflakeQuery.PLATFORM_TYPE:
                        deserialized_queries.append(SnowflakeQuery.from_dict(query_dict))
                    elif platform_type == BigQueryQuery.PLATFORM_TYPE:
                        deserialized_queries.append(BigQueryQuery.from_dict(query_dict))
                    else:
                        raise SerializationError(f'Invalid platform type - {platform_type}')

        return deserialized_queries

    @staticmethod
    def _include_end_date(end_date: datetime) -> Optional[datetime]:
        if end_date is not None and (end_date.hour, end_date.minute, end_date.second) == (0, 0, 0):
            return end_date + timedelta(hours=23, minutes=59, seconds=59)

        return end_date

    def extract_queries(self, start_date: datetime, end_date: datetime) -> [Query]:
        if is_flight_mode_on():
            queries = self._deserialize_query_history()
        else:
            queries = self._query_history_table(start_date, end_date)

            self._serialize_query_history(queries)

        return queries

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [Query]:
        pass

    def get_database_name(self) -> str:
        return self._profile_database_name

    def get_schema_name(self) -> Optional[str]:
        return self._profile_schema_name if not self._ignore_schema else None

    def properties(self) -> dict:
        pass
