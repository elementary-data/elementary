import os
from pathlib import Path

import google.auth
from dateutil import tz
from google.auth.exceptions import DefaultCredentialsError

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.exceptions.exceptions import InvalidArgumentsError
from elementary.monitor import dbt_project_utils
from elementary.utils.ordered_yaml import OrderedYaml


class Config:
    _SLACK = "slack"
    _AWS = "aws"
    _GOOGLE = "google"
    _CONFIG_FILE_NAME = "config.yml"

    # Quoting env vars
    _DATABASE_QUOTING = "DATABASE_QUOTING"
    _SCHEMA_QUOTING = "SCHEMA_QUOTING"
    _IDENTIFIER_QUOTING = "IDENTIFIER_QUOTING"
    _QUOTING_KEY_MAPPING = {
        "database": _DATABASE_QUOTING,
        "schema": _SCHEMA_QUOTING,
        "identifier": _IDENTIFIER_QUOTING,
    }
    _QUOTING_VALID_KEYS = set(_QUOTING_KEY_MAPPING.keys())
    _QUOTING_ENV_VARS = set(_QUOTING_KEY_MAPPING.values())

    DEFAULT_CONFIG_DIR = str(Path.home() / ".edr")

    def __init__(
        self,
        config_dir: str = DEFAULT_CONFIG_DIR,
        profiles_dir: str = None,
        profile_target: str = None,
        dbt_quoting: bool = None,
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
        env: str = None,
    ):
        self.config_dir = config_dir
        self.profiles_dir = profiles_dir
        self.profile_target = profile_target
        self.env = env

        # Additional env vars supplied to dbt invocations
        self.dbt_env_vars = dict()
        self.dbt_env_vars.update(self._parse_dbt_quoting_to_env_vars(dbt_quoting))

        config = self._load_configuration()

        self.target_dir = self._first_not_none(
            config.get("target-path"),
            os.getcwd(),
        )

        self.update_bucket_website = self._first_not_none(
            update_bucket_website,
            config.get("update_bucket_website"),
            False,
        )

        self.timezone = self._first_not_none(
            timezone,
            config.get("timezone"),
        )

        slack_config = config.get(self._SLACK, {})
        self.slack_webhook = self._first_not_none(
            slack_webhook,
            slack_config.get("notification_webhook"),
        )
        self.slack_token = self._first_not_none(
            slack_token,
            slack_config.get("token"),
        )
        self.slack_channel_name = self._first_not_none(
            slack_channel_name,
            slack_config.get("channel_name"),
        )
        self.is_slack_workflow = self._first_not_none(
            slack_config.get("workflows"),
            False,
        )

        aws_config = config.get(self._AWS, {})
        self.aws_profile_name = self._first_not_none(
            aws_profile_name,
            aws_config.get("profile_name"),
        )
        self.s3_bucket_name = self._first_not_none(
            s3_bucket_name, aws_config.get("s3_bucket_name")
        )
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        google_config = config.get(self._GOOGLE, {})
        self.google_project_name = self._first_not_none(
            google_project_name,
            google_config.get("project_name"),
        )
        self.google_service_account_path = self._first_not_none(
            google_service_account_path,
            google_config.get("service_account_path"),
        )
        self.gcs_bucket_name = self._first_not_none(
            gcs_bucket_name,
            google_config.get("gcs_bucket_name"),
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
    def has_s3(self):
        return self.s3_bucket_name

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
        self._validate_internal_dbt_project()
        self._validate_timezone()
        if not self.has_slack:
            raise InvalidArgumentsError(
                "Either a Slack token and a channel or a Slack webhook is required."
            )

    def validate_report(self):
        self._validate_internal_dbt_project()

    def validate_send_report(self):
        self._validate_internal_dbt_project()
        if not self.has_send_report_platform:
            raise InvalidArgumentsError(
                "You must provide a platform to upload the report to (Slack token / S3 / GCS)."
            )

    def _validate_internal_dbt_project(self):
        dbt_runner = DbtRunner(
            dbt_project_utils.PATH,
            self.profiles_dir,
            self.profile_target,
            dbt_env_vars=self.dbt_env_vars,
        )
        dbt_runner.debug(quiet=True)

    def _validate_timezone(self):
        if self.timezone and not tz.gettz(self.timezone):
            raise InvalidArgumentsError("An invalid timezone was provided.")

    @staticmethod
    def _first_not_none(*values):
        return next((v for v in values if v is not None), None)

    @classmethod
    def _parse_dbt_quoting_to_env_vars(cls, dbt_quoting):
        if dbt_quoting is None:
            return {}

        if dbt_quoting == "all":
            return {env_var: "True" for env_var in cls._QUOTING_ENV_VARS}
        elif dbt_quoting == "none":
            return {env_var: "False" for env_var in cls._QUOTING_ENV_VARS}

        dbt_quoting_keys = {part.strip() for part in dbt_quoting.split(",")}
        if not dbt_quoting_keys.issubset(cls._QUOTING_VALID_KEYS):
            raise InvalidArgumentsError(
                "Invalid quoting specification: %s" % dbt_quoting
            )

        env_vars = {env_var: "False" for env_var in cls._QUOTING_ENV_VARS}
        env_vars.update(
            {cls._QUOTING_KEY_MAPPING[key]: "True" for key in dbt_quoting_keys}
        )

        return env_vars
