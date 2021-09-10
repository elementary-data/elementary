from datetime import datetime, timedelta
from lineage.utils import is_flight_mode_on
import json
import os


class QueryHistory(object):

    INFORMATION_SCHEMA_QUERY_HISTORY = """
    select query_text
      from table(information_schema.query_history(
        end_time_range_start=>to_timestamp_ltz(?),
        {end_time_range_end_expr})) 
        where execution_status = 'SUCCESS'
        order by end_time;
    """

    LATEST_QUERY_HISTORY_FILE = './latest_query_history.json'

    def __init__(self, con, should_serialize_query_history: bool = True) -> None:
        self.con = con
        self.should_serialize_query_history = should_serialize_query_history

    def _serialize_query_history(self, queries: [str]) -> None:
        if self.should_serialize_query_history:
            with open(self.LATEST_QUERY_HISTORY_FILE, 'w') as query_history_file:
                json.dump(queries, query_history_file)

    def _deserialize_query_history(self) -> [str]:
        queries = []
        if os.path.exists(self.LATEST_QUERY_HISTORY_FILE):
            with open(self.LATEST_QUERY_HISTORY_FILE, 'r') as query_history_file:
                queries = json.load(query_history_file)
        return queries

    @staticmethod
    def _include_end_date(end_date: datetime) -> datetime:
        if (end_date.hour, end_date.minute, end_date.second) == (0, 0, 0):
            return end_date + timedelta(hours=23, minutes=59, seconds=59)

        return end_date

    def extract_queries(self, start_date: datetime, end_date: datetime) -> [str]:
        queries = []

        if is_flight_mode_on():
            queries = self._deserialize_query_history()
        else:
            with self.con.cursor() as cursor:
                if end_date is None:
                    cursor.execute(self.INFORMATION_SCHEMA_QUERY_HISTORY.
                                   format(end_time_range_end_expr='end_time_range_end=>'
                                                                  'to_timestamp_ltz(current_timestamp())'),
                                   (start_date,))
                else:
                    end_date = self._include_end_date(end_date)
                    cursor.execute(self.INFORMATION_SCHEMA_QUERY_HISTORY.
                                   format(end_time_range_end_expr='end_time_range_end=>to_timestamp_ltz(?)'),
                                   (start_date, end_date))

                rows = cursor.fetchall()
                for row in rows:
                    queries.append(row[0])

            self._serialize_query_history(queries)

        return queries
