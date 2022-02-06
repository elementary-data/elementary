from datetime import datetime, timedelta
from typing import Optional, Union
from lineage.bigquery_query import BigQueryQuery
from exceptions.exceptions import SerializationError
from lineage.query import Query
from lineage.query_history_stats import QueryHistoryStats
from lineage.snowflake_query import SnowflakeQuery
from utils.env_vars import is_flight_mode_on
import json
import os


class QueryHistory(object):

    INFORMATION_SCHEMA_QUERY_HISTORY = None
    PLATFORM_TYPE = None
    SUCCESS_QUERIES_FILE = './latest_query_history.json'
    FAILED_QUERIES_FILE = './failed_queries.json'

    def __init__(self, con, dbs: str, should_export_query_history: bool = True, full_table_names: bool = True) -> None:
        self._con = con
        self._dbs = self.str_to_list(dbs)
        self._should_export_query_history = should_export_query_history
        self._full_table_names = full_table_names
        self._query_history_stats = QueryHistoryStats()
        self.success_queries = []
        self.failed_queries = []

    @staticmethod
    def str_to_list(dbs_str: str) -> Union[list, None]:
        if dbs_str is None:
            return None

        return [db.strip() for db in dbs_str.split(',')]

    @staticmethod
    def _normalize_database_name(db: str) -> str:
        return db.lower().replace('-', '_').replace(' ', '').replace('`', '').replace('"', '')

    @staticmethod
    def serialize_queries_to_file(filename: str, queries: [Query]) -> None:
        with open(filename, 'w') as queries_file:
            serialized_queries = []
            for query in queries:
                serialized_queries.append(query.to_dict())
            json.dump(serialized_queries, queries_file)

    def _serialize_query_history(self) -> None:
        if self._should_export_query_history:
            self.serialize_queries_to_file(self.SUCCESS_QUERIES_FILE, self.success_queries)
            self.serialize_queries_to_file(self.FAILED_QUERIES_FILE, self.failed_queries)

    def _deserialize_query_history(self) -> [Query]:
        if os.path.exists(self.SUCCESS_QUERIES_FILE):
            with open(self.SUCCESS_QUERIES_FILE, 'r') as query_history_file:
                queries = json.load(query_history_file)
                for query_dict in queries:
                    platform_type = query_dict.pop('platform_type')
                    if platform_type == SnowflakeQuery.PLATFORM_TYPE:
                        query = SnowflakeQuery.from_dict(query_dict)
                    elif platform_type == BigQueryQuery.PLATFORM_TYPE:
                        query = BigQueryQuery.from_dict(query_dict)
                    else:
                        raise SerializationError(f'Invalid platform type - {platform_type}')
                    self.add_query(query)

    @staticmethod
    def _include_end_date(end_date: datetime) -> Optional[datetime]:
        if end_date is not None and (end_date.hour, end_date.minute, end_date.second) == (0, 0, 0):
            return end_date + timedelta(hours=23, minutes=59, seconds=59)

        return end_date

    def add_query(self, query: Query):
        if query.parse(self._full_table_names):
            self.success_queries.append(query)
        else:
            self.failed_queries.append(query)
        self._query_history_stats.update_stats(query.query_context)

    def extract_queries(self, start_date: datetime, end_date: datetime) -> [Query]:
        if is_flight_mode_on():
            self._deserialize_query_history()
        else:
            self._query_history_table(start_date, end_date)
            self._serialize_query_history()

        return self.success_queries

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [Query]:
        pass

    def properties(self) -> dict:
        failed_queries_count = len(self.failed_queries)
        success_queries_count = len(self.success_queries)
        queries_count = success_queries_count + failed_queries_count
        query_history_properties = {'query_history_properties': {'failed_queries': failed_queries_count,
                                                                 'success_queries': success_queries_count,
                                                                 'queries_count': queries_count,
                                                                 'platform_type': self.PLATFORM_TYPE}}
        query_history_properties.update({'query_stats': self._query_history_stats.to_dict()})
        return query_history_properties

