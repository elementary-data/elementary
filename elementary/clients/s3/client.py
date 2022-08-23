from os import path
from typing import Optional

import boto3
import botocore.exceptions

from elementary.config.config import Config
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class S3Client:
    def __init__(self, config: Config):
        self.config = config
        aws_session = boto3.Session(profile_name=config.aws_profile_name,
                                    aws_access_key_id=config.aws_access_key_id,
                                    aws_secret_access_key=config.aws_secret_access_key)
        self.client = aws_session.client('s3')

    @classmethod
    def create_client(cls, config: Config) -> Optional['S3Client']:
        return cls(config) if config.has_aws else None

    def send_report(self, html_path: str) -> bool:
        report_filename = path.basename(html_path)
        try:
            self.client.upload_file(html_path, self.config.s3_bucket_name, report_filename,
                                    ExtraArgs={'ContentType': 'text/html'})
            logger.info('Uploaded report to S3.')
            if self.config.update_bucket_website:
                self.client.put_bucket_website(
                    Bucket=self.config.s3_bucket_name,
                    WebsiteConfiguration={'IndexDocument': {'Suffix': report_filename}}
                )
                logger.info("Updated S3 bucket's website.")
        except botocore.exceptions.ClientError:
            logger.error('Failed to upload report to S3.')
            return False
        return True
