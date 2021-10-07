import sqlparse
from sqllineage.core import LineageAnalyzer, LineageResult
from sqllineage.models import Schema, Table
from typing import Optional
from sqllineage.exceptions import SQLLineageException
from lineage.query_context import QueryContext
from lineage.utils import get_logger

logger = get_logger(__name__)


class Query(object):

    def __init__(self, raw_query_text: str, query_context: QueryContext, profile_database_name: str,
                 profile_schema_name: str, full_table_names: bool) -> None:
        self._raw_query_text = raw_query_text
        self._profile_database_name = profile_database_name
        self._profile_schema_name = profile_schema_name
        self._show_full_table_name = full_table_names
        self._query_context = query_context
        self.dropped_tables = set()
        self.renamed_tables = set()
        self.source_tables = set()
        self.target_tables = set()

    @staticmethod
    def _parse_query_text(query_text: str) -> [LineageResult]:
        parsed_query = sqlparse.parse(query_text.strip())
        analyzed_statements = [LineageAnalyzer().analyze(statement) for statement in parsed_query
                               if statement.token_first(skip_cm=True, skip_ws=True)]
        return analyzed_statements

    @staticmethod
    def _resolve_table_qualification(table: Table, database_name: str, schema_name: str) -> Table:
        if not table.schema:
            if database_name is not None and schema_name is not None:
                table.schema = Schema(f'{database_name}.{schema_name}')
        else:
            parsed_query_schema_name = str(table.schema)
            if '.' not in parsed_query_schema_name:
                # Resolved schema is either empty or fully qualified with db_name.schema_name
                if database_name is not None:
                    table.schema = Schema(f'{database_name}.{parsed_query_schema_name}')
                else:
                    table.schema = Schema()
        return table

    def _should_ignore_table(self, table: Table) -> bool:
        if self._profile_schema_name is not None:
            if str(table.schema) == str(Schema(f'{self._profile_database_name}.{self._profile_schema_name}')):
                return False
        else:
            if str(Schema(self._profile_database_name)) in str(table.schema):
                return False

        return True

    def _name_qualification(self, table: Table, database_name: str, schema_name: str) -> Optional[str]:
        table = self._resolve_table_qualification(table, database_name, schema_name)

        if self._should_ignore_table(table):
            return None

        if self._show_full_table_name:
            return str(table)

        return str(table).rsplit('.', 1)[-1]

    def parse(self):
        try:
            database_name = self._query_context.queried_database
            schema_name = self._query_context.queried_schema
            analyzed_statements = self._parse_query_text(self._raw_query_text)
            for analyzed_statement in analyzed_statements:
                # Handle drop tables, if they exist in the statement
                dropped_tables = analyzed_statement.drop
                for dropped_table in dropped_tables:
                    self.dropped_tables.add(self._name_qualification(dropped_table, database_name, schema_name))

                # Handle rename tables
                renamed_tables = analyzed_statement.rename
                for old_table, new_table in renamed_tables:
                    old_table_name = self._name_qualification(old_table, database_name, schema_name)
                    new_table_name = self._name_qualification(new_table, database_name, schema_name)
                    self.renamed_tables.add((old_table_name, new_table_name))

                # sqllineage lib marks CTEs as intermediate tables. Remove CTEs (WITH statements) from the source tables.
                self.source_tables = {self._name_qualification(source, database_name, schema_name)
                                      for source in analyzed_statement.read - analyzed_statement.intermediate}
                self.target_tables = {self._name_qualification(target, database_name, schema_name)
                                      for target in analyzed_statement.write}
        except SQLLineageException as exc:
            logger.debug(f'SQLLineageException was raised while parsing this query -\n{self._raw_query_text}\n'
                         f'Error was -\n{exc}.')

