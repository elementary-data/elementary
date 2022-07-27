from config.config import Config
from exceptions.exceptions import ConfigError
from lineage.bigquery_query_history import BigQueryQueryHistory
from lineage.query_history import QueryHistory
from lineage.snowflake_query_history import SnowflakeQueryHistory

# This requires the installations of both adapters.
from utils.dbt.bigquery import get_bigquery_client
from utils.dbt.snowflake import get_snowflake_client


class QueryHistoryFactory:
    @staticmethod
    def create_query_history(config: 'Config', dbs: str) -> QueryHistory:
        if config.platform == 'snowflake':
            snowflake_con = get_snowflake_client(config.credentials, server_side_binding=False)
            return SnowflakeQueryHistory(con=snowflake_con, dbs=dbs, query_history_source=config.query_history_source)
        elif config.platform == 'bigquery':
            bigquery_client = get_bigquery_client(config.credentials)
            return BigQueryQueryHistory(con=bigquery_client, dbs=dbs)
        else:
            raise ConfigError("Unsupported profile type")
