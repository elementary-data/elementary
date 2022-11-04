from os import path
from typing import Optional

import boto3
import botocore.exceptions

from elementary.config.config import Config
from elementary.utils import bucket_path
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class S3Client:
    def __init__(self, config: Config):
        self.config = config
        aws_session = boto3.Session(
            profile_name=config.aws_profile_name,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
        self.client = aws_session.client("s3")

    @classmethod
    def create_client(cls, config: Config) -> Optional["S3Client"]:
        return cls(config) if config.has_s3 else None

    def send_report(
        self, local_html_file_path: str, remote_bucket_file_path: Optional[str] = None
    ) -> bool:
        report_filename = (
            bucket_path.basename(remote_bucket_file_path)
            if remote_bucket_file_path
            else path.basename(local_html_file_path)
        )
        bucket_report_path = (
            remote_bucket_file_path if remote_bucket_file_path else report_filename
        )
        logger.info(f'Uploading to S3 bucket "{self.config.s3_bucket_name}"')
        try:
            self.client.upload_file(
                local_html_file_path,
                self.config.s3_bucket_name,
                bucket_report_path,
                ExtraArgs={"ContentType": "text/html"},
            )
            logger.info("Uploaded report to S3.")
            if self.config.update_bucket_website:
                self.client.put_bucket_website(
                    Bucket=self.config.s3_bucket_name,
                    # We use report_filename because a path can not be an IndexDocument Suffix.
                    WebsiteConfiguration={"IndexDocument": {"Suffix": report_filename}},
                )
                logger.info("Updated S3 bucket's website.")
        except botocore.exceptions.ClientError:
            logger.exception("Failed to upload report to S3.")
            return False
        return True
