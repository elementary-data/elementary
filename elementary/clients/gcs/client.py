from os import path
from typing import Optional

import google
from google.cloud import storage

from elementary.config.config import Config
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class GCSClient:
    def __init__(self, config: Config):
        self.config = config
        self.client = storage.Client.from_service_account_json(config.google_service_account_path)

    @classmethod
    def create_client(cls, config: Config) -> Optional['GCSClient']:
        return cls(config) if config.has_gcs else None

    def send_report(self, html_path: str) -> bool:
        report_filename = path.basename(html_path)
        try:
            bucket = self.client.get_bucket(self.config.gcs_bucket_name)
            blob = bucket.blob(report_filename)
            blob.upload_from_filename(html_path, content_type='text/html')
            logger.info('Uploaded report to GCS.')
            if self.config.update_bucket_website:
                bucket.copy_blob(blob, bucket, 'index.html')
                logger.info("Updated GCS bucket's website.")
        except google.cloud.exceptions.GoogleCloudError:
            logger.error('Failed to upload report to GCS.')
            return False
        return True
