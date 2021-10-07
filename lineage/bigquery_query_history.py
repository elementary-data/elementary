from datetime import datetime
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

    def __init__(self, con, should_export_query_history: bool = True, dataset: str = None) -> None:
        self.dataset = dataset
        super().__init__(con, should_export_query_history)

    def _query_history_table(self, start_date: datetime, end_date: datetime) -> [tuple]:
        queries = []
        job = self.con.query(self.QUERY_HISTORY)
        logger.debug("Finished executing snowflake history query")
        rows = job.result()
        for row in rows:
            queries.append((row[0], QueryContext(None, None, row[1], row[2], row[3], row[4])))
        logger.debug("Finished fetching snowflake history query results")

        return queries

    def get_database_name(self):
        return self.con.project

    def get_schema_name(self):
        return self.dataset
