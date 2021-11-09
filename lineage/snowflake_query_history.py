from datetime import datetime, date, timedelta

from snowflake.connector.cursor import SnowflakeCursor

from lineage.exceptions import ConfigError
from lineage.query import Query
from lineage.query_context import QueryContext
from lineage.query_history import QueryHistory
from lineage.snowflake_query import SnowflakeQuery
from lineage.utils import get_logger

logger = get_logger(__name__)


class SnowflakeQueryHistory(QueryHistory):
    PLATFORM_TYPE = 'snowflake'

    # Note: Here we filter permissively on the configured database_name, basically finding all the queries that are
    # relevant to this database. Snowflake's query history might show in the database_name column a different db name
    # than the db name that was part of the query. During the parsing logic in the lineage graph we strictly analyze if
    # the query was really executed on the configured db and filter it accordingly.

    INFORMATION_SCHEMA_QUERY_HISTORY = """
    select query_text, database_name, schema_name, end_time, rows_produced, query_type, user_name, role_name, total_elapsed_time
      from table(information_schema.query_history(
        end_time_range_start=>to_timestamp_ltz(:2),
        {end_time_range_end_expr},
        result_limit=>10000)) 
        where execution_status = 'SUCCESS' and query_type not in 
        ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 
        'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE') and
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
    select query_text, database_name, schema_name, end_time, rows_inserted + rows_produced, query_type, user_name, 
    role_name, total_elapsed_time
        from snowflake.account_usage.query_history 
        where end_time >= :2 and {end_time_range_end_expr} 
    and execution_status = 'SUCCESS' and query_type not in 
    ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES',
    'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE') and
    (query_text not ilike '%.query_history%') and
    (contains(collate(query_text, 'en-ci'), collate(:1, 'en-ci')) or collate(database_name, 'en-ci') = :1)
    order by end_time;
    """
    ACCOUNT_USAGE_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'end_time <= current_timestamp()'
    ACCOUNT_USAGE_END_TIME_UP_TO_PARAMETER = 'end_time <= :3'
    QUERY_HISTORY_SOURCE_ACCOUNT_USAGE = 'account_usage'

    def __init__(self, con, profile_database_name: str, profile_schema_name: str,
                 should_export_query_history: bool = True, ignore_schema: bool = False,
                 full_table_names: bool = False, query_history_source: str = None) -> None:
        self.query_history_source = query_history_source.strip().lower() if query_history_source is not None else None
        super().__init__(con, profile_database_name, profile_schema_name, should_export_query_history, ignore_schema,
                         full_table_names)

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

    def _enrich_history_with_view_definitions(self, cursor: SnowflakeCursor, database_name: str, schema_name: str) -> None:
        cursor.execute(self.INFORMATION_SCHEMA_VIEWS, (database_name, ))
        rows = cursor.fetchall()
        for row in rows:
            query_context = QueryContext(queried_database=row[1],
                                         queried_schema=row[2],
                                         query_time=row[3],
                                         query_type='CREATE_VIEW',
                                         role_name=row[4])

            query = SnowflakeQuery(raw_query_text=row[0],
                                   query_context=query_context,
                                   profile_database_name=database_name,
                                   profile_schema_name=schema_name)

            self.add_query(query)

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> None:
        database_name = self.get_database_name()
        schema_name = self.get_schema_name()

        logger.debug(f"Pulling snowflake query history from database - {database_name} and schema - {schema_name}")

        with self._con.cursor() as cursor:
            query_text, bindings = self._build_history_query(start_date, end_date, database_name,
                                                             self.query_history_source)
            cursor.execute(query_text, bindings)
            logger.debug("Finished executing snowflake history query")
            rows = cursor.fetchall()
            for row in rows:
                query_context = QueryContext(queried_database=row[1],
                                             queried_schema=row[2],
                                             query_time=row[3],
                                             query_volume=row[4],
                                             query_type=row[5],
                                             user_name=row[6],
                                             role_name=row[7],
                                             duration=row[8])

                query = SnowflakeQuery(raw_query_text=row[0],
                                       query_context=query_context,
                                       profile_database_name=database_name,
                                       profile_schema_name=schema_name)

                self.add_query(query)

            self._enrich_history_with_view_definitions(cursor, database_name, schema_name)
            logger.debug("Finished fetching snowflake history query results")

    def properties(self) -> dict:
        query_history_properties = super().properties()
        query_history_properties.update({'query_history_source': self.query_history_source})
        return query_history_properties

