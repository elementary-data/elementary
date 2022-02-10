from datetime import datetime, date, timedelta
from typing import Union

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

    INFORMATION_SCHEMA_QUERY_HISTORY_AND_VIEWS = """
    {database_name_normalized}_query_history as (
        select 
            query_text, 
            database_name, 
            schema_name, 
            end_time,
            rows_produced, 
            query_type, 
            user_name, 
            role_name, 
            total_elapsed_time, 
            query_id,
            null as target_table,
            null as source_tables
      from table({database_name}.information_schema.query_history(end_time_range_start=>to_timestamp_ltz(%(start_date)s),
        {end_time_range_end_expr}))
        where execution_status = 'SUCCESS' and query_type not in 
        ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 'GET_FILES',
         'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE', 'ALTER_NETWORK_POLICY', 'ALTER_ACCOUNT', 
         'ALTER_TABLE_DROP_CLUSTERING_KEY', 'ALTER_USER',  'CREATE_CUSTOMER_ACCOUNT', 'CREATE_NETWORK_POLICY', 
         'CREATE_ROLE', 'CREATE_USER', 'DESCRIBE_QUERY', 'DROP_NETWORK_POLICY', 'DROP_ROLE', 'DROP_USER', 'LIST_FILES',
         'REMOVE_FILES', 'REVOKE','UNKNOWN', 'DELETE', 'SELECT') and
        (query_text not ilike '%%.query_history%%') and
        (lower(query_text) like '%%{database_name}%%' 
         or lower(database_name) = '{database_name}')
    ),
    {database_name_normalized}_views as (
        select 
            view_definition as query_text,
            table_catalog as database_name,
            table_schema as schema_name,
            last_altered as end_time,
            null as rows_produced,
            'CREATE_VIEW' as query_type,
            table_owner as user_name,
            null as role_name,
            null as total_elapsed_time,
            null as query_id,
            null as target_table,
            null as source_tables
        from {database_name}.information_schema.views
        where lower(table_schema) != 'information_schema' 
              and view_definition is not NULL
              and (lower(view_definition) like '%%{database_name}%%' 
                   or lower(table_catalog) = '{database_name}')
    ),
    {database_name_normalized}_history_and_views as (
        select * from {database_name_normalized}_query_history union all select * from {database_name_normalized}_views
    ),
    """

    SELECT_FROM_INFORMATION_SCHEMA_QUERY_HISTORY_AND_VIEWS = "select * from " \
                                                             "{database_name_normalized}_history_and_views"

    UNION_ALL_DBS = """
    union_all_dbs as (
        {union_all_dbs}
    )
    select * from union_all_dbs
    order by end_time
    """

    INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'end_time_range_end=>to_timestamp_ltz(current_timestamp())'
    INFO_SCHEMA_END_TIME_UP_TO_PARAMETER = 'end_time_range_end=>to_timestamp_ltz(%(end_date)s)'
    QUERY_HISTORY_SOURCE_INFORMATION_SCHEMA = 'information_schema'

    ACCOUNT_USAGE_QUERY_HISTORY = """
    with source_access_history as (
        select *
        from snowflake.account_usage.access_history
        where query_start_time >= %(start_date)s  
        qualify row_number() over (partition by query_id order by query_id) = 1
    ),
    
    source_query_history as (
        select *
        from snowflake.account_usage.query_history
        where end_time >= %(start_date)s 
        and {end_time_range_end_expr}
        and (lower(query_text) like any (%(database_names_in_like_statement)s) 
             or lower(database_name) in (%(database_names)s))
        qualify row_number() over (partition by query_id order by query_id) = 1
    
    ),
    
    account_usage_views as (
        select * 
        from snowflake.account_usage.views
        where (lower(view_definition) like any (%(database_names_in_like_statement)s)
        or lower(table_catalog) in (%(database_names)s))
        and deleted is null 
        and view_definition is not NULL
      
    ),
    
    access_history as (
         select
             src.query_id,
             lower(replace(direct.value:"objectName"::varchar, '"','')) as direct_access_table_name,
             lower(replace(modified.value:"objectName"::varchar, '"','')) as modified_table_name
         from source_access_history as src,
            lateral flatten(input => src.direct_objects_accessed) as direct,
            lateral flatten(input => src.objects_modified) as modified
         where modified_table_name is not null and direct_access_table_name != modified_table_name       
    ),
    
    query_history as (
        select
            query_id,
            database_name,
            schema_name,
            query_text,
            query_type,
            role_name,
            user_name,
            rows_inserted + rows_updated as rows_produced,
            end_time,
            total_elapsed_time
        from source_query_history
        where is_client_generated_statement = false
              and (lower(query_text) not ilike '%%.query_history%%')
              and execution_status = 'SUCCESS'
              and query_type not in
              ('SHOW', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM',
                'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE', 'ALTER_NETWORK_POLICY', 'ALTER_ACCOUNT',
                'ALTER_TABLE_DROP_CLUSTERING_KEY', 'ALTER_USER',  'CREATE_CUSTOMER_ACCOUNT', 'CREATE_NETWORK_POLICY',
                'CREATE_ROLE', 'CREATE_USER', 'DESCRIBE_QUERY', 'DROP_NETWORK_POLICY', 'DROP_ROLE', 'DROP_USER', 
                'LIST_FILES', 'REMOVE_FILES', 'REVOKE')
     ),
     
     views as (
        select 
            view_definition as query_text,
            table_catalog as database_name,
            table_schema as schema_name,
            last_altered as end_time,
            null as rows_produced,
            'CREATE_VIEW' as query_type,
            null as user_name,
            table_owner as role_name,
            null as total_elapsed_time,
            null as query_id,
            table_name as target_table,
            null as source_tables
       from account_usage_views
       where lower(table_schema) != 'information_schema'
     ),
     
    access_and_query_history as (
        select
            qh.query_text,
            qh.database_name,
            qh.schema_name,
            qh.end_time,
            qh.rows_produced,
            qh.query_type,
            qh.user_name,
            qh.role_name,
            qh.total_elapsed_time,
            qh.query_id,
            ah.modified_table_name as target_table,
            array_agg(distinct ah.direct_access_table_name) as source_tables
        from query_history as qh
            left join access_history as ah
            on (qh.query_id = ah.query_id)
       group by 1,2,3,4,5,6,7,8,9,10,11
      ),
      
    final as (
        select * from access_and_query_history
        union all
        select * from views
    )
    
    select * from final
    order by end_time
    """

    ACCOUNT_USAGE_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'end_time <= current_timestamp()'
    ACCOUNT_USAGE_END_TIME_UP_TO_PARAMETER = 'end_time <= %(end_date)s'
    QUERY_HISTORY_SOURCE_ACCOUNT_USAGE = 'account_usage'

    def __init__(self, con, dbs: Union[str, None], should_export_query_history: bool = True,
                 full_table_names: bool = True, query_history_source: str = None) -> None:
        self.query_history_source = query_history_source.strip().lower() if query_history_source is not None else None
        self.access_history_queries = 0
        super().__init__(con, dbs, should_export_query_history, full_table_names)

    @classmethod
    def _info_schema_query_history(cls, start_date: datetime, end_date: datetime, dbs: list) -> (str, dict):
        if start_date.date() <= date.today() - timedelta(days=7):
            raise ConfigError(f"Cannot retrieve data from more than 7 days ago when pulling history from "
                              f"{cls.QUERY_HISTORY_SOURCE_INFORMATION_SCHEMA}, "
                              f"use {cls.QUERY_HISTORY_SOURCE_ACCOUNT_USAGE} instead "
                              f"(see https://docs.elementary-data.com for more details).")

        logger.debug("Pulling snowflake query history from information schema")
        if end_date is None:
            end_time_range_end_expr = cls.INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP
            params = {'start_date': start_date}
        else:
            end_time_range_end_expr = cls.INFO_SCHEMA_END_TIME_UP_TO_PARAMETER
            params = {'start_date': start_date,
                      'end_date': cls._include_end_date(end_date)}

        query_text = 'with'
        for db in dbs:
            query_text += cls.INFORMATION_SCHEMA_QUERY_HISTORY_AND_VIEWS. \
                format(database_name_normalized=cls._normalize_database_name(db),
                       database_name=db.lower(),
                       end_time_range_end_expr=end_time_range_end_expr)

        dbs_count = len(dbs)
        union_all_dbs = cls.SELECT_FROM_INFORMATION_SCHEMA_QUERY_HISTORY_AND_VIEWS. \
            format(database_name_normalized=cls._normalize_database_name(dbs[0]))
        for i in range(1, dbs_count):
            union_all_dbs += ' union all ' + cls.SELECT_FROM_INFORMATION_SCHEMA_QUERY_HISTORY_AND_VIEWS. \
                format(database_name_normalized=cls._normalize_database_name(dbs[i]))

        query_text += cls.UNION_ALL_DBS.format(union_all_dbs=union_all_dbs)
        return query_text, params

    @classmethod
    def _account_usage_query_history(cls, start_date: datetime, end_date: datetime, dbs: list) -> (str, dict):
        logger.debug("Pulling snowflake query history from account usage")
        if end_date is None:
            query_text = cls.ACCOUNT_USAGE_QUERY_HISTORY.format(end_time_range_end_expr=
                                                                cls.ACCOUNT_USAGE_END_TIME_UP_TO_CURRENT_TIMESTAMP)
            params = {'start_date': start_date,
                      'database_names': [db.lower() for db in dbs],
                      'database_names_in_like_statement': [f'%{db.lower()}%' for db in dbs]}
        else:
            query_text = cls.ACCOUNT_USAGE_QUERY_HISTORY.format(end_time_range_end_expr=
                                                                cls.ACCOUNT_USAGE_END_TIME_UP_TO_PARAMETER)
            params = {'start_date': start_date,
                      'end_date': cls._include_end_date(end_date),
                      'database_names': [db.lower() for db in dbs],
                      'database_names_in_like_statement': [f'%{db.lower()}%' for db in dbs]}

        return query_text, params

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> None:
        logger.debug(f"Pulling snowflake query history for the following databases - {self._dbs}")

        with self._con.cursor() as cursor:
            if self.query_history_source == self.QUERY_HISTORY_SOURCE_ACCOUNT_USAGE:
                query_text, params = self._account_usage_query_history(start_date, end_date, self._dbs)
            else:
                query_text, params = self._info_schema_query_history(start_date, end_date, self._dbs)

            with ThreadSpinner(title='Pulling query history from Snowflake'):
                cursor.execute(query_text, params)
                logger.debug(f"Fetching results from Snowflake")
                rows = cursor.fetchall()

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
                                             query_id=row[9],
                                             destination_table=row[10],
                                             referenced_tables=row[11])

                query = SnowflakeQuery(raw_query_text=row[0],
                                       query_context=query_context)

                self.add_query(query)
                # This is mainly used for debugging
                if query_context.destination_table is not None and query_context.referenced_tables is not None:
                    self.access_history_queries += 1
            logger.debug("Finished fetching snowflake history query results")

    def properties(self) -> dict:
        query_history_properties = super().properties()
        query_history_properties['query_history_properties'].update(
            {'query_history_source': self.query_history_source, 'access_history_queries': self.access_history_queries})
        return query_history_properties

