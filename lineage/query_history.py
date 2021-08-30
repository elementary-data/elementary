from datetime import timedelta

# TODO: check timezone (to_timestamp Vs to_timestamp_ltz), validate escaping
QUERY_HISTORY = """
select query_text
  from table(elementary_db.information_schema.query_history(
    end_time_range_start=>to_timestamp('{query_start_time}'),
    end_time_range_end=>to_timestamp('{query_end_time}')));
"""


def extract_quries_from_query_history(con, start_date, end_date):
    query_end_time = end_date + timedelta(hours=23, minutes=59, seconds=59)
    # Load recent queries from history log
    queries = []
    with con.cursor() as cursor:
        cursor.execute(QUERY_HISTORY.format(query_start_time=start_date, query_end_time=query_end_time))
        rows = cursor.fetchall()
        for row in rows:
            queries.append(row[0])
    return queries
