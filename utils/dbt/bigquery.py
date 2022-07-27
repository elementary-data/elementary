import google.cloud.bigquery
import google.cloud.exceptions
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from google.api_core import client_info


def get_bigquery_client(profile_credentials):
    if profile_credentials.impersonate_service_account:
        creds = \
            BigQueryConnectionManager.get_impersonated_bigquery_credentials(profile_credentials)
    else:
        creds = BigQueryConnectionManager.get_bigquery_credentials(profile_credentials)

    database = profile_credentials.database
    location = getattr(profile_credentials, 'location', None)

    info = client_info.ClientInfo(user_agent=f'elementary')
    return google.cloud.bigquery.Client(
        database,
        creds,
        location=location,
        client_info=info,
    )
