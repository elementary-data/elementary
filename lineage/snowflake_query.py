from lineage.query_context import QueryContext
from lineage.table_resolver import TableResolver
from lineage.query import Query
from lineage.utils import get_logger
import re

logger = get_logger(__name__)

DOLLAR_SIGN_REGEX = re.compile(r'(\b\S+)(\$)(\S+\b)')
DOLLAR_SIGN_PLACEHOLDER = '__dollar_sign__'


class SnowflakeQuery(Query):
    PLATFORM_TYPE = 'SNOWFLAKE'

    @staticmethod
    def from_dict(query_dict: dict):
        query_context = QueryContext.from_dict(query_dict.pop('query_context'))
        if 'platform_type' in query_dict:
            query_dict.pop('platform_type')
        return SnowflakeQuery(**query_dict, query_context=query_context)

    @staticmethod
    def replace_dollar_sign_with_placeholder(query_text: str) -> str:
        return re.sub(DOLLAR_SIGN_REGEX, f'\\1{DOLLAR_SIGN_PLACEHOLDER}\\3', query_text)

    @staticmethod
    def revert_dollar_sign_placeholder(name: str) -> str:
        return name.replace(DOLLAR_SIGN_PLACEHOLDER, '$')

    def parse(self, full_table_names: bool = False) -> bool:
        try:
            table_resolver = TableResolver(self._profile_database_name, self._profile_schema_name,
                                           self.query_context.queried_database, self.query_context.queried_schema,
                                           full_table_names, self.revert_dollar_sign_placeholder)

            # sqlparse library doesn't behave nicely when there is a $ sign in the table name. Therefore we replace it
            # with a placeholder (and revert it back later on using our table resolver)
            self.source_tables, self.target_tables, self.renamed_tables, self.dropped_tables = \
                self._parse_query_text(table_resolver,
                                       self.replace_dollar_sign_with_placeholder(self._raw_query_text))

            return True

        except Exception as exc:
            logger.debug(f'Exception was raised while parsing this query -\n{self._raw_query_text}\n'
                         f'Error was -\n{exc}.')
        return False

