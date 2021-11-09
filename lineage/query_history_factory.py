from typing import Any

from lineage.bigquery_query_history import BigQueryQueryHistory
from lineage.dbt_utils import extract_credentials_and_data_from_profiles, get_bigquery_client
from lineage.exceptions import ConfigError
from lineage.query_history import QueryHistory
from lineage.snowflake_query_history import SnowflakeQueryHistory
import snowflake.connector

snowflake.connector.paramstyle = 'numeric'


class QueryHistoryFactory(object):

    def __init__(self, export_query_history: bool, ignore_schema: bool = False,
                 full_table_names: bool = False) -> None:
        self._export_query_history = export_query_history
        self._ignore_schema = ignore_schema
        self._full_table_names = full_table_names

    def create_query_history(self, credentials: Any, profile_data: dict) -> QueryHistory:
        credentials_type = credentials.type
        if credentials_type == 'snowflake':
            snowflake_con = snowflake.connector.connect(
                account=credentials.account,
                user=credentials.user,
                database=credentials.database,
                schema=credentials.schema,
                warehouse=credentials.warehouse,
                role=credentials.role,
                autocommit=True,
                client_session_keep_alive=credentials.client_session_keep_alive,
                application='elementary',
                **credentials.auth_args()
            )

            return SnowflakeQueryHistory(snowflake_con, credentials.database, credentials.schema,
                                         self._export_query_history, self._ignore_schema, self._full_table_names,
                                         profile_data.get('query_history_source'))
        elif credentials_type == 'bigquery':
            bigquery_client = get_bigquery_client(credentials)
            return BigQueryQueryHistory(bigquery_client, credentials.database, credentials.schema,
                                        self._export_query_history, self._ignore_schema, self._full_table_names)
        else:
            raise ConfigError("Unsupported profile type")
