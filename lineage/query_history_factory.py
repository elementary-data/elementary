from typing import Any

from lineage.bigquery_query_history import BigQueryQueryHistory
from utils.dbt import get_bigquery_client, get_snowflake_client
from exceptions.exceptions import ConfigError
from lineage.query_history import QueryHistory
from lineage.snowflake_query_history import SnowflakeQueryHistory


class QueryHistoryFactory(object):

    def __init__(self, export_query_history: bool, ignore_schema: bool = False,
                 full_table_names: bool = False) -> None:
        self._export_query_history = export_query_history
        self._ignore_schema = ignore_schema
        self._full_table_names = full_table_names

    def create_query_history(self, credentials: Any, profile_data: dict) -> QueryHistory:
        credentials_type = credentials.type
        if credentials_type == 'snowflake':
            snowflake_con = get_snowflake_client(credentials)
            return SnowflakeQueryHistory(snowflake_con, credentials.database, credentials.schema,
                                         self._export_query_history, self._ignore_schema, self._full_table_names,
                                         profile_data.get('query_history_source'))
        elif credentials_type == 'bigquery':
            bigquery_client = get_bigquery_client(credentials)
            return BigQueryQueryHistory(bigquery_client, credentials.database, credentials.schema,
                                        self._export_query_history, self._ignore_schema, self._full_table_names)
        else:
            raise ConfigError("Unsupported profile type")
