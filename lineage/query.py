import sqlparse
from sqllineage.core import LineageAnalyzer, LineageResult
from sqllineage.exceptions import SQLLineageException
from lineage.TableResolver import TableResolver
from lineage.query_context import QueryContext
from lineage.utils import get_logger

logger = get_logger(__name__)


class Query(object):

    def __init__(self, raw_query_text: str, query_context: QueryContext, profile_database_name: str,
                 profile_schema_name: str) -> None:
        self._raw_query_text = raw_query_text
        self._query_context = query_context
        self._profile_database_name = profile_database_name
        self._profile_schema_name = profile_schema_name
        self.dropped_tables = set()
        self.renamed_tables = set()
        self.source_tables = set()
        self.target_tables = set()

    def to_dict(self) -> dict:
        return {'raw_query_text': self._raw_query_text,
                'query_context': self._query_context.to_dict(),
                'profile_database_name': self._profile_database_name,
                'profile_schema_name': self._profile_schema_name}

    @staticmethod
    def from_dict(query_dict: dict):
        query_context = QueryContext.from_dict(query_dict.pop('query_context'))
        return Query(**query_dict, query_context=query_context)

    @staticmethod
    def _parse_query_text(query_text: str) -> [LineageResult]:
        parsed_query = sqlparse.parse(query_text.strip())
        analyzed_statements = [LineageAnalyzer().analyze(statement) for statement in parsed_query
                               if statement.token_first(skip_cm=True, skip_ws=True)]
        return analyzed_statements

    def get_context_as_html(self) -> str:
        return self._query_context.to_html()

    def parse(self, full_table_names: bool = False):
        try:
            table_resolver = TableResolver(self._profile_database_name, self._profile_schema_name,
                                           self._query_context.queried_database, self._query_context.queried_schema,
                                           full_table_names)

            analyzed_statements = self._parse_query_text(self._raw_query_text)
            for analyzed_statement in analyzed_statements:
                # Handle drop tables, if they exist in the statement
                dropped_tables = analyzed_statement.drop
                for dropped_table in dropped_tables:
                    self.dropped_tables.add(table_resolver.name_qualification(dropped_table))

                # Handle rename tables
                renamed_tables = analyzed_statement.rename
                for old_table, new_table in renamed_tables:
                    old_table_name = table_resolver.name_qualification(old_table)
                    new_table_name = table_resolver.name_qualification(new_table)
                    self.renamed_tables.add((old_table_name, new_table_name))

                # sqllineage lib marks CTEs as intermediate tables. Remove CTEs (WITH statements) from the source
                # tables.
                self.source_tables = {table_resolver.name_qualification(source)
                                      for source in analyzed_statement.read - analyzed_statement.intermediate}

                self.target_tables = {table_resolver.name_qualification(target) for target in analyzed_statement.write}

        except SQLLineageException as exc:
            logger.debug(f'SQLLineageException was raised while parsing this query -\n{self._raw_query_text}\n'
                         f'Error was -\n{exc}.')

