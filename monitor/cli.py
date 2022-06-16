import click
import os
from os.path import expanduser

from utils.package import get_package_version
from tracking.anonymous_tracking import AnonymousTracking, track_cli_start, track_cli_exception, track_cli_end
from utils.ordered_yaml import OrderedYaml
from config.config import Config
from monitor.data_monitoring import DataMonitoring
from clients.slack.slack_client import SlackClient

yaml = OrderedYaml()


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
            'full_refresh_dbt_package': full_refresh_dbt_package,
            'version': get_package_version()}


@click.group(invoke_without_command=True)
@click.option(
    '--days-back', '-d',
    type=int,
    default=7,
    help="Set a limit to how far back edr should look for new alerts"
)
@click.option(
    '--slack-webhook', '-s',
    type=str,
    default=None,
    help="A slack webhook URL for sending alerts to a specific channel (also could be configured once in config.yml)"
)
@click.option(
    '--slack-token', '-t',
    type=str,
    default=None,
    help="A slack token for sending alerts over slack (also could be configured once in config.yml)"
)
@click.option(
    '--slack-channel-name', '-ch',
    type=str,
    default=None,
    help="The slack channel which all alerts will be sent to (also could be configured once in config.yml)"
)
@click.option(
    '--config-dir', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr'),
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-p',
    type=str,
    default=os.path.join(expanduser('~'), '.dbt'),
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
    full_refresh_dbt_package
):
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")
    if ctx.invoked_subcommand is not None:
        return
    config = Config(config_dir, profiles_dir)
    anonymous_tracking = AnonymousTracking(config)
    track_cli_start(anonymous_tracking, 'monitor', get_cli_properties(), ctx.command.name)
    try:
        data_monitoring = DataMonitoring(
            config=config,
            update_dbt_package=update_dbt_package,
            slack_webhook=slack_webhook,
            slack_token=slack_token,
            slack_channel_name=slack_channel_name
        )
        data_monitoring.run(days_back, full_refresh_dbt_package)
        track_cli_end(anonymous_tracking, 'monitor', data_monitoring.properties(), ctx.command.name)
    except Exception as exc:
        track_cli_exception(anonymous_tracking, 'monitor', exc, ctx.command.name)
        raise


@monitor.command()
@click.option(
    '--config-dir', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr'),
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-p',
    type=str,
    default=os.path.join(expanduser('~'), '.dbt'),
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
@click.pass_context
def report(ctx, config_dir, profiles_dir, update_dbt_package):
    config = Config(config_dir, profiles_dir)
    anonymous_tracking = AnonymousTracking(config)
    track_cli_start(anonymous_tracking, 'monitor-report', get_cli_properties(), ctx.command.name)
    try:
        data_monitoring = DataMonitoring(config, update_dbt_package)
        data_monitoring.generate_report()
        track_cli_end(anonymous_tracking, 'monitor-report', data_monitoring.properties(), ctx.command.name)
    except Exception as exc:
        track_cli_exception(anonymous_tracking, 'monitor-report', exc, ctx.command.name)
        raise
    return


@monitor.command()
@click.option(
    '--config-dir', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr'),
    help="Global settings for edr are configured in a config.yml file in this directory "
         "(if your config dir is HOME_DIR/.edr, no need to provide this parameter as we use it as default)."
)
@click.option(
    '--profiles-dir', '-p',
    type=str,
    default=os.path.join(expanduser('~'), '.dbt'),
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
    '--slack-webhook', '-s',
    type=str,
    default=None,
    help="A slack webhook URL for sending alerts to a specific channel (also could be configured once in config.yml)"
)
@click.option(
    '--slack-token', '-t',
    type=str,
    default=None,
    help="A slack token for sending alerts over slack (also could be configured once in config.yml)"
)
@click.option(
    '--slack-channel-name', '-ch',
    type=str,
    default=None,
    help="The slack channel which all alerts will be sent to (also could be configured once in config.yml)"
)
@click.pass_context
def send_report(
    ctx,
    config_dir,
    profiles_dir,
    update_dbt_package,
    slack_webhook,
    slack_token,
    slack_channel_name
):
    config = Config(config_dir, profiles_dir)
    anonymous_tracking = AnonymousTracking(config)
    track_cli_start(anonymous_tracking, 'monitor-send-report', get_cli_properties(), ctx.command.name)
    try:
        data_monitoring = DataMonitoring(config, update_dbt_package)
        data_monitoring.generate_report()
        slack_client = SlackClient.initial(token=slack_token, webhook=slack_webhook)
        slack_client.upload_file(
            channel_name=slack_channel_name,
            file_path=os.path.join(config.target_dir, 'elementary.html'), 
            message="Elemantary run report"
        )
    except Exception as exc:
        track_cli_exception(anonymous_tracking, 'monitor-send-report', exc, ctx.command.name)
        raise
    return


if __name__ == "__main__":
    monitor()
