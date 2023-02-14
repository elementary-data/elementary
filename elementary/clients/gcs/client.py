from os import path
from typing import Optional, Tuple

import google
from google.auth.credentials import Credentials
from google.cloud import storage
from google.oauth2 import service_account

from elementary.config.config import Config
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils import bucket_path
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class GCSClient:
    def __init__(self, config: Config, tracking: AnonymousTracking = None):
        self.config = config
        self.client = self.get_client(config)
        self.tracking = tracking

    @classmethod
    def create_client(
        cls, config: Config, tracking: AnonymousTracking = None
    ) -> Optional["GCSClient"]:
        return cls(config, tracking=tracking) if config.has_gcs else None

    def send_report(
        self, local_html_file_path: str, remote_bucket_file_path: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        report_filename = (
            bucket_path.basename(remote_bucket_file_path)
            if remote_bucket_file_path
            else path.basename(local_html_file_path)
        )
        bucket_website_url = None
        bucket_report_path = remote_bucket_file_path or report_filename
        logger.info(f'Uploading to GCS bucket "{self.config.gcs_bucket_name}"')
        try:
            bucket = self.client.get_bucket(self.config.gcs_bucket_name)
            blob = bucket.blob(bucket_report_path)
            blob.upload_from_filename(local_html_file_path, content_type="text/html")
            logger.info("Uploaded report to GCS.")
            if self.config.update_bucket_website:
                bucket_report_folder_path = bucket_path.dirname(bucket_report_path)
                bucket_name = (
                    bucket_path.join_path([bucket_report_folder_path, "index.html"])
                    if bucket_report_folder_path
                    else "index.html"
                )
                bucket.copy_blob(
                    blob=blob,
                    destination_bucket=bucket,
                    new_name=bucket_name,
                )
                bucket_website_url = self.get_bucket_website_url(
                    destination_bucket=self.config.gcs_bucket_name,
                    bucket_name=bucket_name,
                )
                logger.info("Updated GCS bucket's website.")
        except google.cloud.exceptions.GoogleCloudError as ex:
            logger.exception("Failed to upload report to GCS.")
            if self.tracking:
                self.tracking.record_cli_internal_exception(ex)
            return False, bucket_website_url
        return True, bucket_website_url

    def get_bucket_website_url(
        self, bucket_name: str, destination_bucket: Optional[str] = None
    ) -> Optional[str]:
        bucket_website_url = None
        if self.config.update_bucket_website:
            full_bucket_path = (
                f"{destination_bucket}/{bucket_name}"
                if destination_bucket
                else bucket_name
            )
            bucket_website_url = f"https://storage.googleapis.com/{full_bucket_path}"
        return bucket_website_url

    def get_client(self, config: Config):
        creds = self.get_credentials(config)
        if config.google_project_name:
            return storage.Client(config.google_project_name, credentials=creds)
        return storage.Client(credentials=creds)

    @staticmethod
    def get_credentials(config: Config) -> Credentials:
        if config.google_service_account_path:
            return service_account.Credentials.from_service_account_file(
                config.google_service_account_path
            )
        credentials, _ = google.auth.default()
        return credentials
