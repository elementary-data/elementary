from os import path
from typing import Optional, Tuple

import boto3

from elementary.config.config import Config
from elementary.tracking.tracking_interface import Tracking
from elementary.utils import bucket_path
from elementary.utils.log import get_logger

logger = get_logger(__name__)


class S3Client:
    def __init__(self, config: Config, tracking: Optional[Tracking] = None):
        self.config = config
        aws_session = boto3.Session(
            profile_name=config.aws_profile_name,
            region_name=config.aws_region_name,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            aws_session_token=config.aws_session_token,
        )
        self.client = aws_session.client("s3", endpoint_url=config.s3_endpoint_url)
        self.tracking = tracking

    @classmethod
    def create_client(
        cls, config: Config, tracking: Optional[Tracking] = None
    ) -> Optional["S3Client"]:
        return cls(config, tracking=tracking) if config.has_s3 else None

    def send_report(
        self, local_html_file_path: str, remote_bucket_file_path: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        report_filename = (
            bucket_path.basename(remote_bucket_file_path)
            if remote_bucket_file_path
            else path.basename(local_html_file_path)
        )
        bucket_report_path = remote_bucket_file_path or report_filename
        bucket_website_url = None
        logger.info(f'Uploading to S3 bucket "{self.config.s3_bucket_name}"')

        extra_args = {"ContentType": "text/html"}
        if self.config.s3_acl is not None:
            extra_args["ACL"] = self.config.s3_acl
        self.client.upload_file(
            local_html_file_path,
            self.config.s3_bucket_name,
            bucket_report_path,
            ExtraArgs=extra_args,
        )
        logger.info("Uploaded report to S3.")
        if self.config.update_bucket_website:
            self.client.put_bucket_website(
                Bucket=self.config.s3_bucket_name,
                # We use report_filename because a path can not be an IndexDocument Suffix.
                WebsiteConfiguration={"IndexDocument": {"Suffix": report_filename}},
            )
            bucket_website_url = self.get_bucket_website_url()
            logger.info("Updated S3 bucket's website.")
        return True, bucket_website_url

    def get_bucket_website_url(self) -> Optional[str]:
        bucket_website_url = None
        if self.config.update_bucket_website:
            if self.config.report_url:
                bucket_website_url = self.config.report_url
            else:
                try:
                    bucket_name = self.config.s3_bucket_name
                    bucket_location = self._get_bucket_region(bucket_name)
                    aws_s3_website_url = self._get_aws_s3_website_url_from_location(
                        bucket_location
                    )
                    bucket_website_url = f"http://{bucket_name}.{aws_s3_website_url}"

                except Exception as ex:
                    logger.warning(f"Unable to get bucket website URL: {ex}.")
                    bucket_website_url = None
        return bucket_website_url

    def _get_bucket_region(self, bucket_name: str) -> str:
        region = self.client.get_bucket_location(Bucket=bucket_name)[
            "LocationConstraint"
        ]
        if region is None:
            # Specifically for us-east-1, the LocationConstraint is always None
            region = "us-east-1"

        return region

    @staticmethod
    def _get_aws_s3_website_url_from_location(location: str) -> str:
        location_to_website_url_map = {
            "us-east-2": "s3-website.us-east-2.amazonaws.com",
            "us-east-1": "s3-website-us-east-1.amazonaws.com",
            "us-west-1": "s3-website-us-west-1.amazonaws.com",
            "us-west-2": "s3-website-us-west-2.amazonaws.com",
            "af-south-1": "s3-website.af-south-1.amazonaws.com",
            "ap-east-1": "s3-website.ap-east-1.amazonaws.com",
            "ap-south-2": "s3-website.ap-south-2.amazonaws.com",
            "ap-southeast-3": "s3-website.ap-southeast-3.amazonaws.com",
            "ap-south-1": "s3-website.ap-south-1.amazonaws.com",
            "ap-northeast-3": "s3-website.ap-northeast-3.amazonaws.com",
            "ap-northeast-2": "s3-website.ap-northeast-2.amazonaws.com",
            "ap-southeast-1": "s3-website-ap-southeast-1.amazonaws.com",
            "ap-southeast-2": "s3-website-ap-southeast-2.amazonaws.com",
            "ap-northeast-1": "s3-website-ap-northeast-1.amazonaws.com",
            "ca-central-1": "s3-website.ca-central-1.amazonaws.com",
            "cn-northwest-1": "s3-website.cn-northwest-1.amazonaws.com",
            "eu-central-1": "s3-website.eu-central-1.amazonaws.com",
            "eu-west-1": "s3-website-eu-west-1.amazonaws.com",
            "eu-west-2": "s3-website.eu-west-2.amazonaws.com",
            "eu-south-1": "s3-website.eu-south-1.amazonaws.com",
            "eu-west-3": "s3-website.eu-west-3.amazonaws.com",
            "eu-north-1": "s3-website.eu-north-1.amazonaws.com",
            "eu-south-2": "s3-website.eu-south-2.amazonaws.com",
            "eu-central-2": "s3-website.eu-central-2.amazonaws.com",
            "me-south-1": "s3-website.me-south-1.amazonaws.com",
            "me-central-1": "s3-website.me-central-1.amazonaws.com",
            "sa-east-1": "s3-website-sa-east-1.amazonaws.com",
            "us-gov-east-1": "s3-website.us-gov-east-1.amazonaws.com",
            "us-gov-west-1": "s3-website-us-gov-west-1.amazonaws.com",
        }
        return location_to_website_url_map.get(
            location, f"s3-website.{location}.amazonaws.com"
        )
