from lineage.table_resolver import TableResolver
from lineage.query import Query
from sqllineage.exceptions import SQLLineageException
from lineage.utils import get_logger

logger = get_logger(__name__)


class BigQueryQuery(Query):

    def parse(self, full_table_names: bool = False):
        try:
            table_resolver = TableResolver(self._profile_database_name, self._profile_schema_name,
                                           self._query_context.queried_database, self._query_context.queried_schema,
                                           full_table_names)

            if self._query_context.destination_table is not None:
                project = self._query_context.destination_table['project_id']
                dataset = self._query_context.destination_table['dataset_id']
                table = self._query_context.destination_table['table_id']
                if not table.startswith('anon'):
                    self.target_tables.add(table_resolver.name_qualification(f'{project}.{dataset}.{table}'))

            if self._query_context.referenced_tables is not None:
                for referenced_table in self._query_context.referenced_tables:
                    project = referenced_table['project_id']
                    dataset = referenced_table['dataset_id']
                    table = referenced_table['table_id']
                    if not table.startswith('anon'):
                        self.source_tables.add(table_resolver.name_qualification(f'{project}.{dataset}.{table}'))

        except SQLLineageException as exc:
            logger.debug(f'SQLLineageException was raised while parsing this query -\n{self._raw_query_text}\n'
                         f'Error was -\n{exc}.')
        except KeyError as exc:
            logger.debug(f'KeyError was raised while parsing this query -\n{self._raw_query_text}\n'
                         f'Error was -\n{exc}.')
