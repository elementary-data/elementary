import os
from pathlib import Path

import google.auth
from dateutil import tz
from google.auth.exceptions import DefaultCredentialsError

from elementary.exceptions.exceptions import (
    NoElementaryProfileError,
    NoProfilesFileError,
    InvalidArgumentsError,
)
from elementary.utils.ordered_yaml import OrderedYaml


class Config:
    _SLACK = "slack"
    _AWS = "aws"
    _GOOGLE = "google"
    _CONFIG_FILE_NAME = "config.yml"

    DEFAULT_CONFIG_DIR = str(Path.home() / ".edr")
    DEFAULT_PROFILES_DIR = str(Path.home() / ".dbt")

    def __init__(
        self,
        config_dir: str = DEFAULT_CONFIG_DIR,
        profiles_dir: str = DEFAULT_PROFILES_DIR,
        profile_target: str = None,
        update_bucket_website: bool = None,
        slack_webhook: str = None,
        slack_token: str = None,
        slack_channel_name: str = None,
        timezone: str = None,
        aws_profile_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        s3_bucket_name: str = None,
        google_project_name: str = None,
        google_service_account_path: str = None,
        gcs_bucket_name: str = None,
    ):
        self.config_dir = config_dir
        self.profiles_dir = profiles_dir
        self.profile_target = profile_target

        config = self._load_configuration()

        self.target_dir = config.get("target-path") or os.getcwd()

        self.update_bucket_website = update_bucket_website or config.get(
            "update_bucket_website", False
        )
        self.timezone = timezone or config.get("timezone")

        self.slack_webhook = slack_webhook or config.get(self._SLACK, {}).get(
            "notification_webhook"
        )
        self.slack_token = slack_token or config.get(self._SLACK, {}).get("token")
        self.slack_channel_name = slack_channel_name or config.get(self._SLACK, {}).get(
            "channel_name"
        )
        self.is_slack_workflow = config.get(self._SLACK, {}).get("workflows", False)

        self.aws_profile_name = aws_profile_name or config.get(self._AWS, {}).get(
            "profile_name"
        )
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_bucket_name = s3_bucket_name or config.get(self._AWS, {}).get(
            "s3_bucket_name"
        )

        self.google_project_name = google_project_name or config.get(
            self._GOOGLE, {}
        ).get("project_name")
        self.google_service_account_path = google_service_account_path or config.get(
            self._GOOGLE, {}
        ).get("service_account_path")
        self.gcs_bucket_name = gcs_bucket_name or config.get(self._GOOGLE, {}).get(
            "gcs_bucket_name"
        )

        self.anonymous_tracking_enabled = config.get("anonymous_usage_tracking", True)

    def _load_configuration(self) -> dict:
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)
        config_file_path = os.path.join(self.config_dir, self._CONFIG_FILE_NAME)
        if not os.path.exists(config_file_path):
            return {}
        return OrderedYaml().load(config_file_path) or {}

    @property
    def has_send_report_platform(self):
        return (
            (self.slack_token and self.slack_channel_name)
            or self.has_s3
            or self.has_gcs
        )

    @property
    def has_slack(self) -> bool:
        return self.slack_webhook or (self.slack_token and self.slack_channel_name)

    @property
    def has_aws(self) -> bool:
        return self.aws_profile_name or (
            self.aws_access_key_id and self.aws_secret_access_key
        )

    @property
    def has_s3(self):
        return self.s3_bucket_name and self.has_aws

    @property
    def has_gcloud(self):
        if self.google_service_account_path:
            return True
        try:
            google.auth.default()
            return True
        except DefaultCredentialsError:
            return False

    @property
    def has_gcs(self):
        return self.gcs_bucket_name and self.has_gcloud

    def validate_monitor(self):
        self._validate_elementary_profile()
        self._validate_timezone()
        if not self.has_slack:
            raise InvalidArgumentsError(
                "Either a Slack token and a channel or a Slack webhook is required."
            )

    def validate_report(self):
        self._validate_elementary_profile()

    def validate_send_report(self):
        self._validate_elementary_profile()
        if not self.has_send_report_platform:
            raise InvalidArgumentsError(
                "You must provide a platform to upload the report to (Slack token / S3 / GCS)."
            )

    def _validate_elementary_profile(self):
        profiles_path = os.path.join(self.profiles_dir, "profiles.yml")
        try:
            profiles_yml = OrderedYaml().load(profiles_path)
            if "elementary" not in profiles_yml:
                raise NoElementaryProfileError
        except FileNotFoundError:
            raise NoProfilesFileError(self.profiles_dir)

    def _validate_timezone(self):
        if self.timezone and not tz.gettz(self.timezone):
            raise InvalidArgumentsError("An invalid timezone was provided.")
