from datetime import datetime

from lineage.query import Query
from lineage.query_context import QueryContext
from lineage.query_history import QueryHistory
from lineage.utils import get_logger

logger = get_logger(__name__)


class BigQueryQueryHistory(QueryHistory):
    QUERY_HISTORY = """
    SELECT query, end_time, dml_statistics.inserted_row_count + dml_statistics.updated_row_count, statement_type, 
    user_email, destination_table, referenced_tables, job_type, state
           
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
    WHERE
         creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY) AND CURRENT_TIMESTAMP()
         AND job_type = "QUERY"
         AND end_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY) AND CURRENT_TIMESTAMP()
         AND state = "DONE"
    """

    def __init__(self, con, should_export_query_history: bool = True, ignore_schema: bool = False,
                 dataset: str = None) -> None:
        self.dataset = dataset
        super().__init__(con, should_export_query_history, ignore_schema)

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [Query]:
        queries = []
        job = self._con.query(self.QUERY_HISTORY)
        logger.debug("Finished executing snowflake history query")
        rows = job.result()
        for row in rows:
            query_context = QueryContext(query_time=row[1],
                                         query_volume=row[2],
                                         query_type=row[3],
                                         user_name=row[4])

            query = Query(raw_query_text=row[0],
                          query_context=query_context,
                          profile_database_name=self.get_database_name(),
                          profile_schema_name=self.get_schema_name())

            queries.append(query)
        logger.debug("Finished fetching snowflake history query results")

        return queries

    def get_database_name(self):
        return self._con.project

    def get_schema_name(self):
        return self.dataset if not self._ignore_schema else None
