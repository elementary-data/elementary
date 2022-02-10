import sqlparse
from sqllineage.core import LineageAnalyzer, LineageResult
from lineage.table_resolver import TableResolver
from lineage.query_context import QueryContext
from utils.log import get_logger

logger = get_logger(__name__)


class Query(object):
    PLATFORM_TYPE = None

    def __init__(self, raw_query_text: str, query_context: QueryContext) -> None:
        self._raw_query_text = raw_query_text
        self.query_context = query_context
        self.dropped_tables = set()
        self.renamed_tables = set()
        self.source_tables = set()
        self.target_tables = set()

    def to_dict(self) -> dict:
        return {'raw_query_text': self._raw_query_text,
                'query_context': self.query_context.to_dict(),
                'platform_type': self.PLATFORM_TYPE}

    @staticmethod
    def _query_text_to_analyzed_sql_statements(query_text: str) -> [LineageResult]:
        parsed_query = sqlparse.parse(query_text.strip())
        analyzed_statements = [LineageAnalyzer().analyze(statement) for statement in parsed_query
                               if statement.token_first(skip_cm=True, skip_ws=True)]
        return analyzed_statements

    def get_context_as_html(self) -> str:
        return self.query_context.to_html()

    def _parse_platform_specific_queries(self, table_resolver: TableResolver, raw_query_text: str) -> (set, set):
        return set(), set()

    def _parse_query_text(self, table_resolver: TableResolver, raw_query_text: str) -> (set, set, set, set):
        renamed_tables = set()
        dropped_tables = set()

        source_tables, target_tables = self._parse_platform_specific_queries(table_resolver, raw_query_text)
        if len(source_tables) > 0 or len(target_tables) > 0:
            return source_tables, target_tables, renamed_tables, dropped_tables

        analyzed_statements = self._query_text_to_analyzed_sql_statements(raw_query_text)
        for analyzed_statement in analyzed_statements:
            # Handle drop tables, if they exist in the statement
            for dropped_table in analyzed_statement.drop:
                dropped_tables.add(table_resolver.name_qualification(dropped_table))

            # Handle rename tables
            for old_table, new_table in analyzed_statement.rename:
                old_table_name = table_resolver.name_qualification(old_table)
                new_table_name = table_resolver.name_qualification(new_table)
                renamed_tables.add((old_table_name, new_table_name))

            # sqllineage lib marks CTEs as intermediate tables. Remove CTEs (WITH statements) from the source
            # tables.
            if not source_tables:
                source_tables = {table_resolver.name_qualification(source)
                                 for source in analyzed_statement.read - analyzed_statement.intermediate}
            elif len(analyzed_statement.read) > 0:
                logger.debug(f"Unexpected case when source_tables is already filled. Query -\n{raw_query_text}\n")

            if not target_tables:
                target_tables = {table_resolver.name_qualification(target) for target in analyzed_statement.write}
            elif len(analyzed_statement.write) > 0:
                logger.debug(f"Unexpected case when target_tables is already filled. Query -\n{raw_query_text}\n")

        return source_tables, target_tables, renamed_tables, dropped_tables

    def _get_platform_type(self) -> str:
        pass

    def parse(self, full_table_names: bool = False) -> bool:
        pass



