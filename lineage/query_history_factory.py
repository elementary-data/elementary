from typing import Any

from config.config import Config
from lineage.bigquery_query_history import BigQueryQueryHistory
from utils.dbt import get_bigquery_client, get_snowflake_client
from exceptions.exceptions import ConfigError
from lineage.query_history import QueryHistory
from lineage.snowflake_query_history import SnowflakeQueryHistory


class QueryHistoryFactory(object):

    def __init__(self, database_name: str, schema_name: str, export_query_history: bool,
                 full_table_names: bool = False) -> None:
        self.database_name = database_name
        self.schema_name = schema_name
        self._export_query_history = export_query_history
        self._full_table_names = full_table_names

    def create_query_history(self, config: 'Config') -> QueryHistory:
        if config.platform == 'snowflake':
            snowflake_con = get_snowflake_client(config.credentials)
            return SnowflakeQueryHistory(snowflake_con,
                                         self.database_name,
                                         self.schema_name,
                                         self._export_query_history,
                                         self._full_table_names,
                                         config.query_history_source)

        elif config.platform == 'bigquery':
            bigquery_client = get_bigquery_client(config.credentials)
            return BigQueryQueryHistory(bigquery_client,
                                        self.database_name,
                                        self.schema_name,
                                        self._export_query_history,
                                        self._full_table_names)

        else:
            raise ConfigError("Unsupported profile type")
