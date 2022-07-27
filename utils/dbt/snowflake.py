import snowflake.connector


def get_snowflake_client(profile_credentials, server_side_binding=True):
    if server_side_binding:
        snowflake.connector.paramstyle = 'numeric'
    return snowflake.connector.connect(
        account=profile_credentials.account,
        user=profile_credentials.user,
        database=profile_credentials.database,
        schema=profile_credentials.schema,
        warehouse=profile_credentials.warehouse,
        role=profile_credentials.role,
        autocommit=True,
        client_session_keep_alive=profile_credentials.client_session_keep_alive,
        application='elementary',
        **profile_credentials.auth_args()
    )
