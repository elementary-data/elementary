from datetime import datetime, timedelta
from typing import Optional
from lineage.utils import is_flight_mode_on
import json
import os


class QueryHistory(object):

    INFORMATION_SCHEMA_QUERY_HISTORY = None

    LATEST_QUERY_HISTORY_FILE = './latest_query_history.json'

    def __init__(self, con, should_export_query_history: bool = True) -> None:
        self.con = con
        self.should_export_query_history = should_export_query_history

    def _serialize_query_history(self, queries: [str]) -> None:
        if self.should_export_query_history:
            with open(self.LATEST_QUERY_HISTORY_FILE, 'w') as query_history_file:
                json.dump(queries, query_history_file)

    def _deserialize_query_history(self) -> [str]:
        queries = []
        if os.path.exists(self.LATEST_QUERY_HISTORY_FILE):
            with open(self.LATEST_QUERY_HISTORY_FILE, 'r') as query_history_file:
                queries = json.load(query_history_file)
        return queries

    @staticmethod
    def _include_end_date(end_date: datetime) -> Optional[datetime]:
        if end_date is not None and (end_date.hour, end_date.minute, end_date.second) == (0, 0, 0):
            return end_date + timedelta(hours=23, minutes=59, seconds=59)

        return end_date

    def extract_queries(self, start_date: datetime, end_date: datetime) -> [str]:
        if is_flight_mode_on():
            queries = self._deserialize_query_history()
        else:
            queries = self._query_history_table(start_date, end_date)

            self._serialize_query_history(queries)

        return queries

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [tuple]:
        pass

    def get_database_name(self):
        pass

    def get_schema_name(self):
        pass
