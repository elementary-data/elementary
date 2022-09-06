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

    # html_path is the local path where the report generated at.
    # bucket_file_path is where we want to upload the report to at the bucket - support folders!
    def send_report(self, html_path: str, bucket_file_path: Optional[str] = None) -> bool:
        report_filename = path.basename(html_path)
        bucket_report_path = bucket_file_path if bucket_file_path else report_filename
        try:
            bucket = self.client.get_bucket(self.config.gcs_bucket_name)
            blob = bucket.blob(bucket_report_path)
            blob.upload_from_filename(html_path, content_type='text/html')
            logger.info('Uploaded report to GCS.')  
            if self.config.update_bucket_website:
                bucket_report_filder_path = path.dirname(bucket_report_path)
                bucket.copy_blob(
                    blob=blob,
                    destination_bucket=bucket,
                    new_name=path.join(bucket_report_filder_path, 'index.html')
                )
                logger.info("Updated GCS bucket's website.")
        except google.cloud.exceptions.GoogleCloudError:
            logger.error('Failed to upload report to GCS.')
            return False
        return True
