import click
import os
from os.path import expanduser

from monitor.workflows.monitor_workflow import MonitorWorkflow
from monitor.workflows.report_workflow import ReportWorkflow, SendReportWorkflow


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
    '--slack-token', '-st',
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
@click.option(
    '--profile-target', '-t',
    type=str,
    default=None,
    help="if you have multiple targets for Elementary, optionally use this flag to choose a specific target"
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
    profile_target
):
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")
    if ctx.invoked_subcommand is not None:
        return
    
    MonitorWorkflow(
        click_context=ctx,
        module_name="monitor",
        days_back=days_back,
        slack_webhook=slack_webhook,
        slack_token=slack_token,
        slack_channel_name=slack_channel_name,
        config_dir=config_dir,
        profiles_dir=profiles_dir,
        update_dbt_package=update_dbt_package,
        full_refresh_dbt_package=full_refresh_dbt_package,
        profile_target=profile_target,
    ).run()


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
    '--profile-target', '-t',
    type=str,
    default=None,
    help="if you have multiple targets for Elementary, optionally use this flag to choose a specific target"
)
@click.pass_context
def report(ctx, config_dir, profiles_dir, update_dbt_package, profile_target):
    ReportWorkflow(
        click_context=ctx,
        module_name="monitor-report",
        config_dir=config_dir,
        profiles_dir=profiles_dir,
        profile_target=profile_target,
        update_dbt_package=update_dbt_package
    ).run()


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
    '--slack-token', '-st',
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
    '--profile-target', '-t',
    type=str,
    default=None,
    help="if you have multiple targets for Elementary, optionally use this flag to choose a specific target"
)
@click.pass_context
def send_report(
    ctx,
    config_dir,
    profiles_dir,
    update_dbt_package,
    slack_webhook,
    slack_token,
    slack_channel_name,
    profile_target
):
    SendReportWorkflow(
        click_context=ctx,
        module_name="monitor-send-report",
        config_dir=config_dir,
        profiles_dir=profiles_dir,
        profile_target=profile_target,
        slack_webhook=slack_webhook,
        slack_token=slack_token,
        slack_channel_name=slack_channel_name,
        update_dbt_package=update_dbt_package
    ).run()


if __name__ == "__main__":
    monitor()
