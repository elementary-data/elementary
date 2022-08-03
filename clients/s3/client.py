import boto3

from config.config import Config


def get_s3_client(config: Config):
    if not config.has_aws:
        return None
    aws_session = boto3.Session(profile_name=config.aws_profile_name,
                                aws_access_key_id=config.aws_access_key_id,
                                aws_secret_access_key=config.aws_secret_access_key,
                                aws_session_token=config.aws_session_token)
    return aws_session.client('s3')
