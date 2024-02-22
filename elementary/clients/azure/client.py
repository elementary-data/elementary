from os import path
from typing import Optional, Tuple

from azure.storage.blob import BlobServiceClient

from elementary.config.config import Config
from elementary.tracking.tracking_interface import Tracking
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class AzureClient:
    def __init__(self, config: Config, tracking: Optional[Tracking] = None):
        self.config = config
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.config.azure_connection_string
        )
        self.tracking = tracking

    @classmethod
    def create_client(
        cls, config: Config, tracking: Optional[Tracking] = None
    ) -> Optional["AzureClient"]:
        return cls(config, tracking=tracking) if config.has_blob else None

    def send_report(
        self, local_html_file_path: str, remote_bucket_file_path: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        report_filename = (
            remote_bucket_file_path
            if remote_bucket_file_path
            else path.basename(local_html_file_path)
        )
        blob_handle = self.blob_service_client.get_blob_client(
            container=self.config.azure_container_name, blob=report_filename
        )
        bucket_website_url = None
        logger.info(
            f'Uploading to Azure container "{self.config.azure_container_name}"'
        )
        with open(local_html_file_path, "rb") as data:
            blob_handle.upload_blob(data, content_type="text/html", overwrite=True)
        logger.info("Uploaded report to Azure blob storage.")
        if self.config.update_bucket_website:
            dst_blob_client = self.blob_service_client.get_blob_client(
                self.config.azure_container_name, blob="index.html"
            )
            # Copy the uploaded file to the destination path within the same container
            dst_blob_client.start_copy_from_url(blob_handle.url)
            # Get the website URL of the copied file
            bucket_website_url = dst_blob_client.url
            logger.info("Updated Azure container's website.")
        return True, bucket_website_url
