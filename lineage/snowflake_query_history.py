from datetime import datetime, date, timedelta
from lineage.query_history import QueryHistory
from lineage.utils import get_logger

logger = get_logger(__name__)


class SnowflakeQueryHistory(QueryHistory):
    # Note: Here we filter permissively on the configured database_name, basically finding all the queries that are
    # relevant to this database. Snowflake's query history might show in the database_name column a different db name
    # than the db name that was part of the query. During the parsing logic in the lineage graph we strictly analyze if
    # the query was really executed on the configured db and filter it accordingly.

    INFORMATION_SCHEMA_QUERY_HISTORY = """
    select query_text, database_name, schema_name
      from table(information_schema.query_history(
        end_time_range_start=>to_timestamp_ltz(:2),
        {end_time_range_end_expr})) 
        where execution_status = 'SUCCESS' and query_type not in 
        ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 
        'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE') and
        (query_text not ilike '%.query_history%') and 
        (contains(collate(query_text, 'en-ci'), collate(:1, 'en-ci')) or database_name = :1)
        order by end_time;
    """
    INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'end_time_range_end=>to_timestamp_ltz(current_timestamp())'
    INFO_SCHEMA_END_TIME_UP_TO_PARAMETER = 'end_time_range_end=>to_timestamp_ltz(:3)'

    ACCOUNT_USAGE_QUERY_HISTORY = """
    select query_text, database_name, schema_name 
        from snowflake.account_usage.query_history 
        where end_time >= :2 and {end_time_range_end_expr} 
    and execution_status = 'SUCCESS' and query_type not in 
    ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES',
    'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE') and
    (query_text not ilike '%.query_history%') and
    (contains(collate(query_text, 'en-ci'), collate(:1, 'en-ci')) or database_name = :1)
    order by end_time;
    """
    ACCOUNT_USAGE_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'end_time <= current_timestamp()'
    ACCOUNT_USAGE_END_TIME_UP_TO_PARAMETER = 'end_time <= :3'

    @classmethod
    def _build_history_query(cls, start_date: datetime, end_date: datetime, database_name: str) -> (str, tuple):
        if start_date.date() <= date.today() - timedelta(days=7):
            # In case the dates are older than a week ago we will need to pull the history from the account_usage
            logger.debug("Pulling snowflake query history from account usage")
            query = cls.ACCOUNT_USAGE_QUERY_HISTORY
            end_time_up_to_current_timestamp = cls.ACCOUNT_USAGE_END_TIME_UP_TO_CURRENT_TIMESTAMP
            end_time_up_to_parameter = cls.ACCOUNT_USAGE_END_TIME_UP_TO_PARAMETER
        else:
            logger.debug("Pulling snowflake query history from information schema")
            query = cls.INFORMATION_SCHEMA_QUERY_HISTORY
            end_time_up_to_current_timestamp = cls.INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP
            end_time_up_to_parameter = cls.INFO_SCHEMA_END_TIME_UP_TO_PARAMETER

        if end_date is None:
            query = query.format(end_time_range_end_expr=end_time_up_to_current_timestamp)
            bindings = (database_name, start_date,)
        else:
            query = query.format(end_time_range_end_expr=end_time_up_to_parameter)
            bindings = (database_name, start_date, cls._include_end_date(end_date))

        return query, bindings

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [tuple]:
        queries = []
        with self.con.cursor() as cursor:
            query, bindings = self._build_history_query(start_date, end_date, self.get_database_name())
            cursor.execute(query, bindings)
            logger.debug("Finished executing snowflake history query")
            rows = cursor.fetchall()
            for row in rows:
                queries.append((row[0], row[1], row[2]))
            logger.debug("Finished fetching snowflake history query results")

        return queries

    def get_database_name(self):
        return self.con.database

    def get_schema_name(self):
        return self.con.schema
