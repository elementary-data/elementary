from datetime import datetime, date, timedelta
from snowflake.connector.cursor import SnowflakeCursor
from alive_progress import alive_it
from exceptions.exceptions import ConfigError
from lineage.query_context import QueryContext
from lineage.query_history import QueryHistory
from lineage.snowflake_query import SnowflakeQuery
from utils.log import get_logger
from utils.thread_spinner import ThreadSpinner

logger = get_logger(__name__)


class SnowflakeQueryHistory(QueryHistory):
    PLATFORM_TYPE = 'snowflake'

    # Note: Here we filter permissively on the configured database_name, basically finding all the queries that are
    # relevant to this database. Snowflake's query history might show in the database_name column a different db name
    # than the db name that was part of the query. During the parsing logic in the lineage graph we strictly analyze if
    # the query was really executed on the configured db and filter it accordingly.

    INFORMATION_SCHEMA_QUERY_HISTORY = """
    select query_text, database_name, schema_name, end_time, rows_produced, query_type, user_name, role_name, 
    total_elapsed_time, query_id
      from table(information_schema.query_history(
        end_time_range_start=>to_timestamp_ltz(:2),
        {end_time_range_end_expr},
        result_limit=>10000)) 
        where execution_status = 'SUCCESS' and query_type not in 
        ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 'GET_FILES',
         'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE', 'ALTER_NETWORK_POLICY', 'ALTER_ACCOUNT', 
         'ALTER_TABLE_DROP_CLUSTERING_KEY', 'ALTER_USER',  'CREATE_CUSTOMER_ACCOUNT', 'CREATE_NETWORK_POLICY', 
         'CREATE_ROLE', 'CREATE_USER', 'DESCRIBE_QUERY', 'DROP_NETWORK_POLICY', 'DROP_ROLE', 'DROP_USER', 'LIST_FILES',
         'REMOVE_FILES', 'REVOKE','UNKNOWN', 'DELETE', 'SELECT') and
        (query_text not ilike '%.query_history%') and 
        (contains(collate(query_text, 'en-ci'), collate(:1, 'en-ci')) or collate(database_name, 'en-ci') = :1)
        order by end_time;
    """
    INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'end_time_range_end=>to_timestamp_ltz(current_timestamp())'
    INFO_SCHEMA_END_TIME_UP_TO_PARAMETER = 'end_time_range_end=>to_timestamp_ltz(:3)'
    QUERY_HISTORY_SOURCE_INFORMATION_SCHEMA = 'information_schema'

    INFORMATION_SCHEMA_VIEWS = """
    select view_definition, table_catalog, table_schema, last_altered, table_owner 
        from information_schema.views
        where collate(table_catalog, 'en-ci') = :1 and view_definition is not NULL;
    """

    ACCOUNT_USAGE_QUERY_HISTORY = """
    select query_text, database_name, schema_name, end_time, rows_inserted + rows_updated, query_type, user_name, 
    role_name, total_elapsed_time, query_id
        from snowflake.account_usage.query_history 
        where end_time >= :2 and {end_time_range_end_expr} 
        and execution_status = 'SUCCESS' and query_type not in 
        ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 'GET_FILES',
         'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE', 'ALTER_NETWORK_POLICY', 'ALTER_ACCOUNT', 
         'ALTER_TABLE_DROP_CLUSTERING_KEY', 'ALTER_USER',  'CREATE_CUSTOMER_ACCOUNT', 'CREATE_NETWORK_POLICY', 
         'CREATE_ROLE', 'CREATE_USER', 'DESCRIBE_QUERY', 'DROP_NETWORK_POLICY', 'DROP_ROLE', 'DROP_USER', 'LIST_FILES',
         'REMOVE_FILES', 'REVOKE','UNKNOWN', 'DELETE', 'SELECT') and
    (query_text not ilike '%.query_history%') and
    (contains(collate(query_text, 'en-ci'), collate(:1, 'en-ci')) or collate(database_name, 'en-ci') = :1)
    order by end_time;
    """
    ACCOUNT_USAGE_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'end_time <= current_timestamp()'
    ACCOUNT_USAGE_END_TIME_UP_TO_PARAMETER = 'end_time <= :3'
    QUERY_HISTORY_SOURCE_ACCOUNT_USAGE = 'account_usage'

    USE_DATABASE = 'use database IDENTIFIER(:1);'

    def __init__(self, con, database_name: str, schema_name: str,
                 should_export_query_history: bool = True, full_table_names: bool = False,
                 query_history_source: str = None) -> None:
        self.query_history_source = query_history_source.strip().lower() if query_history_source is not None else None
        super().__init__(con, database_name, schema_name, should_export_query_history, full_table_names)

    @classmethod
    def _build_history_query(cls, start_date: datetime, end_date: datetime, database_name: str,
                             query_history_source: str) -> (str, tuple):
        if query_history_source == cls.QUERY_HISTORY_SOURCE_ACCOUNT_USAGE:
            # In case the dates are older than a week ago we will need to pull the history from the account_usage
            logger.debug("Pulling snowflake query history from account usage")
            query = cls.ACCOUNT_USAGE_QUERY_HISTORY
            end_time_up_to_current_timestamp = cls.ACCOUNT_USAGE_END_TIME_UP_TO_CURRENT_TIMESTAMP
            end_time_up_to_parameter = cls.ACCOUNT_USAGE_END_TIME_UP_TO_PARAMETER
        else:
            if start_date.date() <= date.today() - timedelta(days=7):
                raise ConfigError(f"Cannot retrieve data from more than 7 days ago when pulling history from "
                                  f"{cls.QUERY_HISTORY_SOURCE_INFORMATION_SCHEMA}, "
                                  f"use {cls.QUERY_HISTORY_SOURCE_ACCOUNT_USAGE} instead "
                                  f"(see https://docs.elementary-data.com for more details).")

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

    def _enrich_history_with_view_definitions(self, cursor: SnowflakeCursor) -> None:
        spinner = ThreadSpinner(title='Pulling view definitions from Snowflake')
        spinner.start()
        cursor.execute(self.INFORMATION_SCHEMA_VIEWS, (self._database_name, ))
        rows = cursor.fetchall()
        spinner.stop()
        rows_with_progress_bar = alive_it(rows, title="Parsing view definitions")
        for row in rows_with_progress_bar:
            query_context = QueryContext(queried_database=row[1],
                                         queried_schema=row[2],
                                         query_time=row[3],
                                         query_type='CREATE_VIEW',
                                         role_name=row[4])

            query = SnowflakeQuery(raw_query_text=row[0],
                                   query_context=query_context,
                                   database_name=self._database_name,
                                   schema_name=self._schema_name)

            self.add_query(query)

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> None:
        logger.debug(f"Pulling snowflake query history from database - {self._database_name} and schema - "
                     f"{self._schema_name}")

        with self._con.cursor() as cursor:
            cursor.execute(self.USE_DATABASE, (self._database_name,))
            query_text, bindings = self._build_history_query(start_date, end_date, self._database_name,
                                                             self.query_history_source)

            spinner = ThreadSpinner(title='Pulling query history from Snowflake')
            spinner.start()
            cursor.execute(query_text, bindings)
            logger.debug(f"Fetching results from Snowflake")
            rows = cursor.fetchall()
            spinner.stop()

            rows_with_progress_bar = alive_it(rows, title="Parsing queries")
            for row in rows_with_progress_bar:
                query_context = QueryContext(queried_database=row[1],
                                             queried_schema=row[2],
                                             query_time=row[3],
                                             query_volume=row[4],
                                             query_type=row[5],
                                             user_name=row[6],
                                             role_name=row[7],
                                             duration=row[8],
                                             query_id=row[9])

                query = SnowflakeQuery(raw_query_text=row[0],
                                       query_context=query_context,
                                       database_name=self._database_name,
                                       schema_name=self._schema_name)

                self.add_query(query)

            self._enrich_history_with_view_definitions(cursor)
            logger.debug("Finished fetching snowflake history query results")

    def properties(self) -> dict:
        query_history_properties = super().properties()
        query_history_properties['query_history_properties'].update({'query_history_source': self.query_history_source})
        return query_history_properties

