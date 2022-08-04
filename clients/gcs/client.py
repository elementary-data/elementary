from google.cloud import storage

from config.config import Config


def get_gcs_client(config: Config):
    if not config.has_gcs:
        return None
    return storage.Client.from_service_account_json(config.google_service_account_path)
