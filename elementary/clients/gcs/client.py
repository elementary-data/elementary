from os import path
from typing import Optional, Tuple
from urllib.parse import urljoin

import google  # type: ignore[import]
from google.auth.credentials import Credentials  # type: ignore[import]
from google.cloud import storage  # type: ignore[attr-defined, import]
from google.oauth2 import service_account  # type: ignore[import]

from elementary.config.config import Config
from elementary.tracking.tracking_interface import Tracking
from elementary.utils import bucket_path
from elementary.utils.log import get_logger

logger = get_logger(__name__)

DEFAULT_BUCKET_WEBSITE_URL = "https://storage.googleapis.com"
GCS_DEFAULT_TIMEOUT = 60


class GCSClient:
    def __init__(self, config: Config, tracking: Optional[Tracking] = None):
        self.config = config
        self.client = self.get_client(config)
        self.tracking = tracking

    @classmethod
    def create_client(
        cls, config: Config, tracking: Optional[Tracking] = None
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
        bucket = self.client.bucket(self.config.gcs_bucket_name)
        blob = bucket.blob(bucket_report_path)
        blob.upload_from_filename(
            local_html_file_path,
            content_type="text/html",
            timeout=self.config.gcs_timeout_limit or GCS_DEFAULT_TIMEOUT,
        )
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
        return True, bucket_website_url

    def get_bucket_website_url(
        self, bucket_name: str, destination_bucket: Optional[str] = None
    ) -> Optional[str]:
        bucket_website_url = None
        if self.config.update_bucket_website:
            if self.config.report_url:
                bucket_website_url = self.config.report_url
            else:
                full_bucket_path = (
                    f"{destination_bucket}/{bucket_name}"
                    if destination_bucket
                    else bucket_name
                )
                bucket_website_url = urljoin(
                    DEFAULT_BUCKET_WEBSITE_URL, full_bucket_path
                )
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
