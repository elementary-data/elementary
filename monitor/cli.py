import click

from config.config import Config
from monitor.data_monitoring import DataMonitoring
from tracking.anonymous_tracking import AnonymousTracking
from utils.log import get_logger
from utils.ordered_yaml import OrderedYaml

yaml = OrderedYaml()

logger = get_logger(__name__)


def get_cli_properties() -> dict:
    click_context = click.get_current_context()
    if click_context is None:
        return dict()

    params = click_context.params
    if params is None:
        return dict()

    reload_monitoring_configuration = params.get('reload_monitoring_configuration')
    update_dbt_package = params.get('update_dbt_package')
    full_refresh_dbt_package = params.get('full_refresh_dbt_package')

    return {'reload_monitoring_configuration': reload_monitoring_configuration,
            'update_dbt_package': update_dbt_package,
            'full_refresh_dbt_package': full_refresh_dbt_package}


@click.group(invoke_without_command=True)
@click.option(
    '--days-back', '-d',
    type=int,
    default=7,
    help="Set a limit to how far back edr should look for new alerts."
)
@click.option(
    '--slack-webhook', '-s',
    type=str,
    default=None,
    help="A slack webhook URL for sending alerts to a specific channel."
)
@click.option(
    '--slack-token', '-st',
    type=str,
    default=None,
    help="A slack token for sending alerts over slack.",
)
@click.option(
    '--slack-channel-name', '-ch',
    type=str,
    default=None,
    help="The slack channel which all alerts will be sent to.",
)
@click.option(
    '--config-dir', '-c',
    type=str,
    default=Config.DEFAULT_CONFIG_DIR,
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-p',
    type=str,
    default=Config.DEFAULT_PROFILES_DIR,
    help="Specify your profiles dir where a profiles.yml is located, this could be a dbt profiles dir "
         "(if your profiles dir is HOME_DIR/.dbt, no need to provide this parameter as we use it as default).",
)
@click.option(
    '--update-dbt-package', '-u',
    type=bool,
    default=False,
    help="Force downloading the latest version of the edr internal dbt package (usually this is not needed, "
         "see documentation to learn more)."
)
@click.option(
    '--full-refresh-dbt-package', '-f',
    type=bool,
    default=False,
    help="Force running a full refresh of all incremental models in the edr dbt package (usually this is not needed, "
         "see documentation to learn more)."
)
@click.option(
    '--profile-target', '-t',
    type=str,
    default=None,
    help="if you have multiple targets for Elementary, optionally use this flag to choose a specific target."
)
@click.option(
    '--dbt-vars',
    type=str,
    default=None,
    help="Specify raw YAML string of your dbt variables."
)
@click.pass_context
def monitor(
        ctx,
        days_back,
        slack_webhook,
        slack_token,
        slack_channel_name,
        config_dir,
        profiles_dir,
        update_dbt_package,
        full_refresh_dbt_package,
        profile_target,
        dbt_vars
):
    """
    Monitor your warehouse and send alerts to Slack.
    """

    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")
    if ctx.invoked_subcommand is not None:
        return
    vars = yaml.loads(dbt_vars) if dbt_vars else None
    config = Config(config_dir, profiles_dir, profile_target, slack_webhook=slack_webhook, slack_token=slack_token,
                    slack_channel_name=slack_channel_name)
    anonymous_tracking = AnonymousTracking(config)
    anonymous_tracking.track_cli_start('monitor', get_cli_properties(), ctx.command.name)
    try:
        if not config.has_slack:
            logger.error('Either a Slack token and a channel or a Slack webhook is required.')
            return 1

        data_monitoring = DataMonitoring(config=config, force_update_dbt_package=update_dbt_package)
        success = data_monitoring.run(days_back, full_refresh_dbt_package, dbt_vars=vars)
        anonymous_tracking.track_cli_end('monitor', data_monitoring.properties(), ctx.command.name)
        if not success:
            return 1
        return 0
    except Exception as exc:
        anonymous_tracking.track_cli_exception('monitor', exc, ctx.command.name)
        raise


@monitor.command()
@click.option(
    '--days-back', '-d',
    type=int,
    default=7,
    help="Set a limit to how far back Elementary should collect dbt and Elementary results while generating the report."
)
@click.option(
    '--config-dir', '-c',
    type=str,
    default=Config.DEFAULT_CONFIG_DIR,
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-p',
    type=str,
    default=Config.DEFAULT_PROFILES_DIR,
    help="Specify your profiles dir where a profiles.yml is located, this could be a dbt profiles dir "
         "(if your profiles dir is HOME_DIR/.dbt, no need to provide this parameter as we use it as default).",
)
@click.option(
    '--update-dbt-package', '-u',
    type=bool,
    default=False,
    help="Force downloading the latest version of the edr internal dbt package (usually this is not needed, "
         "see documentation to learn more)."
)
@click.option(
    '--profile-target', '-t',
    type=str,
    default=None,
    help="If you have multiple targets for Elementary, optionally use this flag to choose a specific target."
)
@click.option(
    '--executions-limit', '-el',
    type=int,
    default=30,
    help='Set the number of invocations shown for each test in the "Test Runs" report.'
)
@click.option(
    '--file-path',
    type=str,
    help="The file path where Elementary's report will be saved."
)
@click.option(
    '--disable-passed-test-metrics',
    type=bool,
    default=False,
    help="If set to true elementary report won't show data metrics for passed tests (this can improve report creation time)."
)
@click.pass_context
def report(ctx, days_back, config_dir, profiles_dir, update_dbt_package, profile_target, executions_limit, file_path,
           disable_passed_test_metrics):
    """
    Generate a local report of your warehouse.
    """
    config = Config(config_dir, profiles_dir, profile_target)
    anonymous_tracking = AnonymousTracking(config)
    anonymous_tracking.track_cli_start('monitor-report', get_cli_properties(), ctx.command.name)
    try:
        data_monitoring = DataMonitoring(config, update_dbt_package)
        success = data_monitoring.generate_report(tracking=anonymous_tracking, days_back=days_back,
                                                  test_runs_amount=executions_limit, file_path=file_path,
                                                  disable_passed_test_metrics=disable_passed_test_metrics)
        anonymous_tracking.track_cli_end('monitor-report', data_monitoring.properties(), ctx.command.name)
        if not success:
            return 1
        return 0
    except Exception as exc:
        anonymous_tracking.track_cli_exception('monitor-report', exc, ctx.command.name)
        raise


@monitor.command()
@click.option(
    '--slack-token', '-st',
    type=str,
    default=None,
    help="A slack token for sending alerts over slack.",
)
@click.option(
    '--slack-channel-name', '-ch',
    type=str,
    default=None,
    help="The slack channel which all alerts will be sent to.",
)
@click.option(
    '--aws-profile-name',
    type=str,
    default=None,
    help="AWS profile name",
)
@click.option(
    '--aws-access-key-id',
    type=str,
    default=None,
    help="The access key ID for AWS"
)
@click.option(
    '--aws-secret-access-key',
    type=str,
    default=None,
    help="The secret access key for AWS"
)
@click.option(
    '--s3-bucket-name',
    type=str,
    default=None,
    help="The name of the S3 bucket to upload the report to."
)
@click.option(
    '--google-service-account-path',
    type=str,
    default=None,
    help="The path to the Google service account json file"
)
@click.option(
    '--gcs-bucket-name',
    type=str,
    default=None,
    help="The name of the GCS bucket to upload the report to."
)
@click.option(
    '--update-bucket-website',
    type=bool,
    default=False,
    help="Update the bucket's static website with the latest report."
)
@click.option(
    '--days-back', '-d',
    type=int,
    default=7,
    help="Set a limit to how far back Elementary should collect dbt and Elementary results while generating the report."
)
@click.option(
    '--config-dir', '-c',
    type=str,
    default=Config.DEFAULT_CONFIG_DIR,
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-p',
    type=str,
    default=Config.DEFAULT_PROFILES_DIR,
    help="Specify your profiles dir where a profiles.yml is located, this could be a dbt profiles dir "
         "(if your profiles dir is HOME_DIR/.dbt, no need to provide this parameter as we use it as default).",
)
@click.option(
    '--update-dbt-package', '-u',
    type=bool,
    default=False,
    help="Force downloading the latest version of the edr internal dbt package (usually this is not needed, "
         "see documentation to learn more)."
)
@click.option(
    '--profile-target', '-t',
    type=str,
    default=None,
    help="if you have multiple targets for Elementary, optionally use this flag to choose a specific target."
)
@click.option(
    '--executions-limit', '-el',
    type=int,
    default=30,
    help='Set the number of invocations shown for each test in the "Test Runs" report.'
)
@click.option(
    '--disable-passed-test-metrics',
    type=bool,
    default=False,
    help="If set to true elementary report won't show data metrics for passed tests (this can improve report creation time)."
)
@click.pass_context
def send_report(
        ctx,
        days_back,
        config_dir,
        profiles_dir,
        update_dbt_package,
        slack_token,
        slack_channel_name,
        profile_target,
        executions_limit,
        disable_passed_test_metrics,
        update_bucket_website,
        aws_profile_name,
        aws_access_key_id,
        aws_secret_access_key,
        s3_bucket_name,
        google_service_account_path,
        gcs_bucket_name
):
    """
    Send the report to an external platform.
    The current options are Slack, AWS S3, and Google Cloud Storage.
    Each specified platform will be sent a report.
    """

    config = Config(config_dir, profiles_dir, profile_target, slack_token=slack_token,
                    slack_channel_name=slack_channel_name, update_bucket_website=update_bucket_website,
                    aws_profile_name=aws_profile_name, aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key, s3_bucket_name=s3_bucket_name,
                    google_service_account_path=google_service_account_path, gcs_bucket_name=gcs_bucket_name)
    anonymous_tracking = AnonymousTracking(config)
    anonymous_tracking.track_cli_start('monitor-send-report', get_cli_properties(), ctx.command.name)
    try:
        if not config.has_send_report_platform:
            logger.error('You must provide a platform to upload the report to (Slack token / S3 / GCS).')
            return 1

        data_monitoring = DataMonitoring(config=config, force_update_dbt_package=update_dbt_package)
        command_succeeded = False
        generated_report_successfully, elementary_html_path = data_monitoring.generate_report(
            tracking=anonymous_tracking, days_back=days_back, test_runs_amount=executions_limit,
            disable_passed_test_metrics=disable_passed_test_metrics, should_open_browser=False)
        if generated_report_successfully and elementary_html_path:
            command_succeeded = data_monitoring.send_report(elementary_html_path)
        anonymous_tracking.track_cli_end('monitor-send-report', data_monitoring.properties(), ctx.command.name)
        return 0 if command_succeeded else 1

    except Exception as exc:
        anonymous_tracking.track_cli_exception('monitor-send-report', exc, ctx.command.name)
        raise


if __name__ == "__main__":
    monitor()
