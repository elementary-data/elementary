from alive_progress import alive_it
from google.cloud import bigquery

from lineage.bigquery_query import BigQueryQuery
from lineage.query import Query
from lineage.query_context import QueryContext
from lineage.query_history import QueryHistory
from utils.log import get_logger
from datetime import datetime

from utils.thread_spinner import ThreadSpinner

logger = get_logger(__name__)


class BigQueryQueryHistory(QueryHistory):
    PLATFORM_TYPE = 'bigquery'

    INFORMATION_SCHEMA_QUERY_HISTORY = """
    {database_name_normalized}_query_history as (
    SELECT query, end_time, dml_statistics.inserted_row_count + dml_statistics.updated_row_count, statement_type, 
    user_email, destination_table, referenced_tables, TIMESTAMP_DIFF(end_time, start_time, MILLISECOND),
    job_id
           
    FROM `{database_name}.region-{location}.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
    WHERE
         project_id = '{database_name}'
         AND creation_time BETWEEN @start_time AND {creation_time_range_end_expr}
         AND end_time BETWEEN @start_time AND {end_time_range_end_expr}
         AND job_type = "QUERY"
         AND state = "DONE"
         AND error_result is NULL
         AND query NOT like '%JOBS_BY_PROJECT%'
    ),
    """
    INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP = 'CURRENT_TIMESTAMP()'
    INFO_SCHEMA_END_TIME_UP_TO_PARAMETER = '@end_time'

    SELECT_FROM_INFORMATION_SCHEMA_QUERY_HISTORY = "select * from {database_name_normalized}_query_history"

    UNION_ALL_DBS = """
        union_all_dbs as (
            {union_all_dbs}
        )
        select * from union_all_dbs
        order by end_time
    """

    def __init__(self, con, dbs: str, should_export_query_history: bool = True, full_table_names: bool = True) -> None:
        super().__init__(con, dbs, should_export_query_history, full_table_names)

    @classmethod
    def _build_history_query(cls, start_date: datetime, end_date: datetime, dbs: list, location: str) -> \
            (str, []):
        params = [bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_date)]

        end_time_range_end_expr = cls.INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP
        creation_time_range_end_expr = cls.INFO_SCHEMA_END_TIME_UP_TO_CURRENT_TIMESTAMP
        if end_date is not None:
            params.append(bigquery.ScalarQueryParameter("end_time",
                                                        "TIMESTAMP",
                                                        cls._include_end_date(end_date)))
            end_time_range_end_expr = cls.INFO_SCHEMA_END_TIME_UP_TO_PARAMETER
            creation_time_range_end_expr = cls.INFO_SCHEMA_END_TIME_UP_TO_PARAMETER

        query_text = 'with'
        for db in dbs:
            query_text += cls.INFORMATION_SCHEMA_QUERY_HISTORY.\
                format(database_name_normalized=cls._normalize_database_name(db),
                       database_name=db,
                       location=location,
                       creation_time_range_end_expr=creation_time_range_end_expr,
                       end_time_range_end_expr=end_time_range_end_expr)

        dbs_count = len(dbs)
        union_all_dbs = cls.SELECT_FROM_INFORMATION_SCHEMA_QUERY_HISTORY.\
            format(database_name_normalized=cls._normalize_database_name(dbs[0]))
        for i in range(1, dbs_count):
            union_all_dbs += ' union all ' + cls.SELECT_FROM_INFORMATION_SCHEMA_QUERY_HISTORY. \
                format(database_name_normalized=cls._normalize_database_name(dbs[i]))

        query_text += cls.UNION_ALL_DBS.format(union_all_dbs=union_all_dbs)

        return query_text, params

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [Query]:
        logger.debug(f"Pulling BigQuery history from databases - {self._dbs}")

        query_text, query_parameters = self._build_history_query(start_date, end_date, self._dbs, self._con.location)

        job_config = bigquery.QueryJobConfig(
            query_parameters=query_parameters
        )

        with ThreadSpinner(title='Pulling query history from BigQuery'):
            job = self._con.query(query_text, job_config=job_config)
            logger.debug("Finished executing bigquery jobs history query")
            rows = list(job.result())

        rows_with_progress_bar = alive_it(rows, title="Parsing queries")
        for row in rows_with_progress_bar:
            query_context = QueryContext(query_time=row[1],
                                         query_volume=row[2],
                                         query_type=row[3],
                                         user_name=row[4],
                                         destination_table=row[5],
                                         referenced_tables=row[6],
                                         duration=row[7],
                                         query_id=row[8])

            query = BigQueryQuery(raw_query_text=row[0],
                                  query_context=query_context)

            self.add_query(query)

        logger.debug("Finished fetching bigquery history job results")

