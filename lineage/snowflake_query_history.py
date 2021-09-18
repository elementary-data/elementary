from datetime import datetime, date, timedelta
from lineage.query_history import QueryHistory
from lineage.utils import get_logger

logger = get_logger(__name__)


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

    # TODO: Maybe use database id instead of name
    # TODO: when filtering on db name, maybe we should lower the name?

    ACCOUNT_USAGE_QUERY_HISTORY = """
    select query_text, schema_name 
        from snowflake.account_usage.query_history 
        where end_time >= ? and {end_time_range_end_expr} 
    and execution_status = 'SUCCESS' and query_type not in 
    ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES',
    'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE') and database_name = ? 
    order by end_time;
    """

    @classmethod
    def _build_history_query(cls, start_date: datetime, end_date: datetime, database_name: str) -> (str, tuple):
        if start_date.date() <= date.today() - timedelta(days=7):
            # In case the dates are older than a week ago we will need to pull the history from the account_usage
            logger.debug("Pulling snowflake query history from account usage")
            query = cls.ACCOUNT_USAGE_QUERY_HISTORY
            if end_date is None:
                query = query.format(end_time_range_end_expr='end_time <= current_timestamp()')
                bindings = (start_date, database_name)
            else:
                query = query.format(end_time_range_end_expr='end_time <= ?')
                bindings = (start_date, cls._include_end_date(end_date), database_name)
        else:
            logger.debug("Pulling snowflake query history from information schema")
            query = cls.INFORMATION_SCHEMA_QUERY_HISTORY
            if end_date is None:
                query = query.format(end_time_range_end_expr='end_time_range_end=>'
                                                             'to_timestamp_ltz(current_timestamp())')
                bindings = (start_date,)
            else:
                query = query.format(end_time_range_end_expr='end_time_range_end=>to_timestamp_ltz(?)')
                bindings = (start_date, cls._include_end_date(end_date))

        return query, bindings

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [tuple]:
        queries = []
        with self.con.cursor() as cursor:
            query, bindings = self._build_history_query(start_date, end_date, self.get_database_name())
            cursor.execute(query, bindings)
            rows = cursor.fetchall()
            for row in rows:
                queries.append((row[0], row[1]))

        return queries

    def get_database_name(self):
        return self.con.database
