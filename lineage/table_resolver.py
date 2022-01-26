from sqllineage.models import Schema, Table
from typing import Optional, Union, Callable
from utils.log import get_logger

logger = get_logger(__name__)


class TableResolver(object):

    def __init__(self, database_name: str, schema_name: str, queried_database_name: str = None,
                 queried_schema_name: str = None, full_table_names: bool = False,
                 remove_special_char_callback: Callable = None) -> None:
        self._database_name = database_name
        self._schema_name = schema_name
        self._queried_database_name = queried_database_name
        self._queried_schema_name = queried_schema_name
        self._show_full_table_name = full_table_names
        self._remove_special_char_callback = remove_special_char_callback

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
        if self._schema_name is not None:
            if str(table.schema) == str(Schema(f'{self._database_name}.{self._schema_name}')):
                return False
        else:
            if str(Schema(self._database_name)) in str(table.schema):
                return False

        return True

    def name_qualification(self, table: Union[Table, str]) -> Optional[str]:
        if isinstance(table, str):
            table = Table(table)

        # If queried database and schema names exist, prefer them when resolving the table name
        database_name = self._queried_database_name if self._queried_database_name is not None else \
            self._database_name
        schema_name = self._queried_schema_name if self._queried_schema_name is not None else self._schema_name

        table = self._resolve_table_qualification(table, database_name, schema_name)

        if self._should_ignore_table(table):
            return None

        if self._show_full_table_name:
            resolved_table_name = str(table)
        else:
            resolved_table_name = str(table).rsplit('.', 1)[-1]

        if self._remove_special_char_callback is not None:
            resolved_table_name = self._remove_special_char_callback(resolved_table_name)

        logger.debug(f'Resolved table name - {resolved_table_name}')
        return resolved_table_name
