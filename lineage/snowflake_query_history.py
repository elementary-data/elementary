from datetime import datetime
from lineage.query_history import QueryHistory


class SnowflakeQueryHistory(QueryHistory):
    INFORMATION_SCHEMA_QUERY_HISTORY = """
    select query_text, schema_name
      from table(information_schema.query_history(
        end_time_range_start=>to_timestamp_ltz(?),
        {end_time_range_end_expr})) 
        where execution_status = 'SUCCESS' and query_type not in 
        ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 
        'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE')
        order by end_time;
    """

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [tuple]:
        queries = []
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
                queries.append((row[0], row[1]))

        return queries

    def get_database_name(self):
        return self.con.database
