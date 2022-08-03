import os

from utils.ordered_yaml import OrderedYaml


class Config:
    _SLACK = 'slack'
    _AWS = 'aws'
    _CONFIG_FILE_NAME = 'config.yml'

    def __init__(self, config_dir: str, profiles_dir: str, profile_target: str = None, slack_webhook: str = None,
                 slack_token: str = None, slack_channel_name: str = None, aws_profile_name: str = None,
                 aws_access_key_id: str = None, aws_secret_access_key: str = None, s3_bucket_name: str = None):
        self.config_dir = config_dir
        self.profiles_dir = profiles_dir
        self.profile_target = profile_target
        config = self._load_configuration()

        self.target_dir = config.get('target-path') or os.getcwd()

        self.slack_webhook = slack_webhook or config.get(self._SLACK, {}).get('notification_webhook')
        self.slack_token = slack_token or config.get(self._SLACK, {}).get('token')
        self.slack_channel_name = slack_channel_name or config.get(self._SLACK, {}).get('channel_name')
        self.is_slack_workflow = config.get(self._SLACK, {}).get('workflows', False)

        self.aws_profile_name = aws_profile_name or config.get(self._AWS, {}).get('profile_name')
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_bucket_name = s3_bucket_name or config.get(self._AWS, {}).get('s3_bucket_name')

        self.anonymous_tracking_enabled = config.get('anonymous_usage_tracking', True)

        self._validate()

    def _load_configuration(self) -> dict:
        if not os.path.exists(self.config_dir):
            os.makedirs(self.config_dir)
        config_file_path = os.path.join(self.config_dir, self._CONFIG_FILE_NAME)
        if not os.path.exists(config_file_path):
            return {}
        return OrderedYaml().load(config_file_path)

    @property
    def has_slack(self) -> bool:
        return any([self.slack_token, self.slack_webhook])

    @property
    def has_aws(self) -> bool:
        return self.s3_bucket_name and (
                self.aws_profile_name or all([self.aws_access_key_id, self.aws_secret_access_key])
        )

    def _validate(self):
        self.validate_slack()

    def validate_slack(self):
        if self.slack_token and not self.slack_channel_name:
            raise ValueError('Slack channel name is required if token is provided.')
