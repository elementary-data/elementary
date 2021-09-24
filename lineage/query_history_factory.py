from lineage.dbt_utils import extract_credentials_and_data_from_profiles
from lineage.exceptions import ConfigError
from lineage.query_history import QueryHistory
from lineage.snowflake_query_history import SnowflakeQueryHistory
import snowflake.connector

snowflake.connector.paramstyle = 'numeric'


class QueryHistoryFactory(object):

    def __init__(self, profiles_dir: str, profile_name: str, export_query_history: bool) -> None:
        self.profiles_dir = profiles_dir
        self.profile_name = profile_name
        self.export_query_history = export_query_history

    def create_query_history(self) -> QueryHistory:
        credentials, profile_data = extract_credentials_and_data_from_profiles(self.profiles_dir, self.profile_name)
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

            return SnowflakeQueryHistory(snowflake_con, self.export_query_history,
                                         profile_data.get('query_history_source'))
        else:
            raise ConfigError("Unsupported profile type")
