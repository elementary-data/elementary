from datetime import timedelta
from lineage.utils import is_flight_mode_on
import json
import os


class QueryHistoryHandler(object):
    # TODO: check timezone (to_timestamp_ltz), validate escaping
    # TODO: choose db
    QUERY_HISTORY_QUERY = """
    select query_text
      from table(elementary_db.information_schema.query_history(
        end_time_range_start=>to_timestamp_ltz('{query_start_time}'),
        end_time_range_end=>to_timestamp_ltz('{query_end_time}'))) 
        where execution_status = 'SUCCESS'
        order by end_time;
    """

    LATEST_QUERY_HISTORY_FILE = './latest_query_history.json'

    def __init__(self, con, should_serialize_query_history: bool = True) -> None:
        self.con = con
        self.should_serialize_query_history = should_serialize_query_history

    def _serialize_query_history(self, queries) -> None:
        if self.should_serialize_query_history:
            with open(self.LATEST_QUERY_HISTORY_FILE, 'w') as query_history_file:
                json.dump(queries, query_history_file)

    def _deserialize_query_history(self) -> [str]:
        queries = []
        if os.path.exists(self.LATEST_QUERY_HISTORY_FILE):
            with open(self.LATEST_QUERY_HISTORY_FILE, 'r') as query_history_file:
                queries = json.load(query_history_file)
        return queries

    def extract_queries_from_query_history(self, start_date, end_date):

        query_end_time = end_date + timedelta(hours=23, minutes=59, seconds=59)
        # Load recent queries from history log
        queries = []

        if is_flight_mode_on():
            queries = self._deserialize_query_history()
        else:
            with self.con.cursor() as cursor:
                cursor.execute(self.QUERY_HISTORY_QUERY.format(query_start_time=start_date, query_end_time=query_end_time))
                rows = cursor.fetchall()
                for row in rows:
                    queries.append(row[0])

            self._serialize_query_history(queries)

        return queries
