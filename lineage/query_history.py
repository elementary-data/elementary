from datetime import timedelta
from lineage.utils import is_flight_mode_on
import json
import os

# TODO: check timezone (to_timestamp_ltz), validate escaping
# TODO: filter only on success queries
QUERY_HISTORY = """
select query_text
  from table(elementary_db.information_schema.query_history(
    end_time_range_start=>to_timestamp_ltz('{query_start_time}'),
    end_time_range_end=>to_timestamp_ltz('{query_end_time}'))) 
    where execution_status = 'SUCCESS'
    order by end_time;
"""

LATEST_QUERY_HISTORY_FILE = './latest_query_history.json'


def extract_queries_from_query_history(con, start_date, end_date, should_serialize_query_history=True):

    query_end_time = end_date + timedelta(hours=23, minutes=59, seconds=59)
    # Load recent queries from history log
    queries = []

    if is_flight_mode_on() and os.path.exists(LATEST_QUERY_HISTORY_FILE):
        with open(LATEST_QUERY_HISTORY_FILE, 'r') as query_history_file:
            queries = json.load(query_history_file)
    else:
        with con.cursor() as cursor:
            cursor.execute(QUERY_HISTORY.format(query_start_time=start_date, query_end_time=query_end_time))
            rows = cursor.fetchall()
            for row in rows:
                queries.append(row[0])

        if should_serialize_query_history:
            with open(LATEST_QUERY_HISTORY_FILE, 'w') as query_history_file:
                json.dump(queries, query_history_file)

    return queries
