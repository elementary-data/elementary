import os
from pathlib import Path
from typing import Optional

import google.auth  # type: ignore[import]
from dateutil import tz
from google.auth.exceptions import DefaultCredentialsError  # type: ignore[import]

from elementary.exceptions.exceptions import InvalidArgumentsError
from elementary.monitor.alerts.grouping_type import GroupingType
from elementary.utils.ordered_yaml import OrderedYaml

DEFAULT_ENV = "dev"


class Config:
    _SLACK = "slack"
    _AWS = "aws"
    _GOOGLE = "google"
    _AZURE = "azure"
    _TEAMS = "teams"
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

    DEFAULT_TARGET_PATH = os.getcwd() + "/edr_target"

    DEFAULT_GROUP_ALERTS_THRESHOLD = 100

    def __init__(
        self,
        config_dir: str = DEFAULT_CONFIG_DIR,
        profiles_dir: Optional[str] = None,
        project_dir: Optional[str] = None,
        profile_target: Optional[str] = None,
        project_profile_target: Optional[str] = None,
        target_path: str = DEFAULT_TARGET_PATH,
        dbt_quoting: Optional[bool] = None,
        update_bucket_website: Optional[bool] = None,
        slack_webhook: Optional[str] = None,
        slack_token: Optional[str] = None,
        slack_channel_name: Optional[str] = None,
        slack_group_alerts_by: Optional[str] = None,
        group_alerts_threshold: Optional[int] = None,
        timezone: Optional[str] = None,
        aws_profile_name: Optional[str] = None,
        aws_region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        s3_endpoint_url: Optional[str] = None,
        s3_bucket_name: Optional[str] = None,
        s3_acl: Optional[str] = None,
        google_project_name: Optional[str] = None,
        google_service_account_path: Optional[str] = None,
        gcs_bucket_name: Optional[str] = None,
        gcs_timeout_limit: Optional[int] = None,
        azure_connection_string: Optional[str] = None,
        azure_container_name: Optional[str] = None,
        report_url: Optional[str] = None,
        teams_webhook: Optional[str] = None,
        env: str = DEFAULT_ENV,
        run_dbt_deps_if_needed: Optional[bool] = None,
        project_name: Optional[str] = None,
    ):
        self.config_dir = config_dir
        self.profiles_dir = profiles_dir
        self.project_dir = project_dir
        self.profile_target = profile_target
        self.project_profile_target = project_profile_target
        self.env = env
        self.project_name = project_name

        # Additional env vars supplied to dbt invocations
        self.env_vars = dict()
        self.env_vars.update(self._parse_dbt_quoting_to_env_vars(dbt_quoting))

        config = self._load_configuration()

        self.target_dir = self._first_not_none(
            target_path,
            config.get("target-path"),
            os.getcwd(),
        )
        os.makedirs(os.path.abspath(self.target_dir), exist_ok=True)
        os.environ["DBT_LOG_PATH"] = os.path.abspath(target_path)

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
        self.slack_group_alerts_by = self._first_not_none(
            slack_group_alerts_by,
            slack_config.get("group_alerts_by"),
            GroupingType.BY_ALERT.value,
        )
        self.group_alerts_threshold = self._first_not_none(
            group_alerts_threshold,
            slack_config.get("group_alerts_threshold"),
            self.DEFAULT_GROUP_ALERTS_THRESHOLD,
        )

        teams_config = config.get(self._TEAMS, {})
        self.teams_webhook = self._first_not_none(
            teams_webhook,
            teams_config.get("teams_webhook"),
        )

        aws_config = config.get(self._AWS, {})
        self.aws_profile_name = self._first_not_none(
            aws_profile_name,
            aws_config.get("profile_name"),
        )
        self.aws_region_name = self._first_not_none(
            aws_region_name,
            aws_config.get("region_name"),
        )
        self.s3_endpoint_url = self._first_not_none(
            s3_endpoint_url, aws_config.get("s3_endpoint_url")
        )
        self.s3_bucket_name = self._first_not_none(
            s3_bucket_name, aws_config.get("s3_bucket_name")
        )
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.s3_acl = s3_acl

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
        self.gcs_timeout_limit = self._first_not_none(
            gcs_timeout_limit,
            google_config.get("gcs_timeout_limit"),
        )

        azure_config = config.get(self._AZURE, {})
        self.azure_connection_string = self._first_not_none(
            azure_connection_string,
            azure_config.get("azure_connection_string"),
        )
        self.azure_container_name = self._first_not_none(
            azure_container_name,
            azure_config.get("azure_container_name"),
        )

        self.report_url = self._first_not_none(
            report_url,
            aws_config.get("report_url"),
            google_config.get("report_url"),
            azure_config.get("report_url"),
        )

        self.anonymous_tracking_enabled = config.get("anonymous_usage_tracking", True)
        self.run_dbt_deps_if_needed = self._first_not_none(
            run_dbt_deps_if_needed, config.get("run_dbt_deps_if_needed"), True
        )

        self.disable_elementary_version_check = config.get(
            "disable_elementary_version_check", False
        )

        self.disable_elementary_logo_print = config.get(
            "disable_elementary_logo_print", False
        )

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
            or self.has_blob
        )

    @property
    def has_slack(self) -> bool:
        return self.slack_webhook or (self.slack_token and self.slack_channel_name)

    @property
    def has_teams(self) -> bool:
        return self.teams_webhook

    @property
    def has_s3(self):
        return self.s3_bucket_name

    @property
    def has_blob(self):
        return self.azure_container_name

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

    @property
    def specified_env(self) -> Optional[str]:
        return self.env if self.env != DEFAULT_ENV else None

    def validate_monitor(self):
        provided_integrations = list(
            filter(
                lambda provided_integration: provided_integration,
                [self.has_slack, self.has_teams],
            )
        )
        self._validate_timezone()
        if not provided_integrations:
            raise InvalidArgumentsError(
                "Either a Slack token and a channel, a Slack webhook or a Microsoft Teams webhook is required."
            )
        if len(provided_integrations) > 1:
            raise InvalidArgumentsError(
                "You provided both a Slack and Teams integration. Please provide only one so we know where to send the alerts."
            )

    def validate_send_report(self):
        if not self.has_send_report_platform:
            raise InvalidArgumentsError(
                "You must provide a platform to upload the report to (Slack token / S3 / GCS)."
            )

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

    @staticmethod
    def locate_user_project_dir() -> Optional[str]:
        working_dir = Path.cwd()
        if working_dir.joinpath("dbt_project.yml").exists():
            return str(working_dir)
        return None
