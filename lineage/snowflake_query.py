from sqllineage.exceptions import SQLLineageException
from lineage.table_resolver import TableResolver
from lineage.query import Query
from lineage.utils import get_logger

logger = get_logger(__name__)


class SnowflakeQuery(Query):
    def parse(self, full_table_names: bool = False) -> None:
        try:
            table_resolver = TableResolver(self._profile_database_name, self._profile_schema_name,
                                           self._query_context.queried_database, self._query_context.queried_schema,
                                           full_table_names)

            self.source_tables, self.target_tables, self.renamed_tables, self.dropped_tables = \
                self._parse_query_text(table_resolver, self._raw_query_text)
        except SQLLineageException as exc:
            logger.debug(f'SQLLineageException was raised while parsing this query -\n{self._raw_query_text}\n'
                         f'Error was -\n{exc}.')
