import sys

import click

from elementary.cli.e2e.mocks.e2e_data_monitoring_alerts import E2EDataMonitoringAlerts
from elementary.config.config import Config
from elementary.monitor.cli import Command, common_options, get_cli_properties
from elementary.tracking.anonymous_tracking import AnonymousCommandLineTracking
from elementary.utils.ordered_yaml import OrderedYaml

yaml = OrderedYaml()


@click.group(invoke_without_command=True)
@common_options(Command.MONITOR)
@click.option(
    "--slack-webhook",
    "-sw",
    type=str,
    default=None,
    help="A slack webhook URL for sending alerts to a specific channel.",
)
@click.option(
    "--deprecated-slack-webhook",
    "-s",  # Deprecated - will be used for --select in the future
    type=str,
    default=None,
    help="DEPRECATED! - A slack webhook URL for sending alerts to a specific channel.",
)
@click.option(
    "--timezone",
    "-tz",
    type=str,
    default=None,
    help="The timezone of which all timestamps will be converted to. (default is user local timezone)",
)
@click.option(
    "--full-refresh-dbt-package",
    "-f",
    type=bool,
    default=False,
    help="Force running a full refresh of all incremental models in the edr dbt package (usually this is not needed, "
    "see documentation to learn more).",
)
@click.option(
    "--dbt-vars",
    type=str,
    default=None,
    help="Specify raw YAML string of your dbt variables.",
)
@click.option(
    "--test",
    type=bool,
    default=False,
    help="Whether to send a test message in case there are no alerts.",
)
@click.option(
    "--suppression-interval",
    type=int,
    default=0,
    help="The number of hours to suppress alerts after an alert was sent (this is a global default setting).",
)
@click.option(
    "--group-by",
    type=click.Choice(["alert", "table"]),
    default=None,
    help="Whether to group alerts by 'alert' or by 'table'",
)
@click.option(
    "--override-dbt-project-config",
    "-oc",
    is_flag=True,
    help="Whether to override the settings (slack channel, suppression interval) "
    "in the model or test meta in the dbt project with the parameters provided by the CLI.",
)
@click.option(
    "--report-url",
    type=str,
    default=None,
    help="The report URL for the alert attached links.",
)
@click.pass_context
def e2e_monitor(
    ctx,
    days_back,
    slack_webhook,
    deprecated_slack_webhook,
    slack_token,
    slack_channel_name,
    timezone,
    config_dir,
    profiles_dir,
    project_dir,
    update_dbt_package,
    full_refresh_dbt_package,
    dbt_quoting,
    profile_target,
    project_profile_target,
    dbt_vars,
    test,
    disable_samples,
    env,
    select,
    group_by,
    target_path,
    suppression_interval,
    override_dbt_project_config,
    report_url,
):
    """
    Run e2e test for edr monitor command.
    """
    if ctx.invoked_subcommand is not None:
        return
    if deprecated_slack_webhook is not None:
        click.secho(
            '\n"-s" is deprecated and won\'t be supported in the near future.\n'
            'Please use "-sw" or "--slack-webhook" for passing Slack webhook.\n',
            fg="bright_red",
        )
        slack_webhook = deprecated_slack_webhook
    vars = yaml.loads(dbt_vars) if dbt_vars else None
    config = Config(
        config_dir=config_dir,
        profiles_dir=profiles_dir,
        project_dir=project_dir,
        profile_target=profile_target,
        project_profile_target=project_profile_target,
        target_path=target_path,
        dbt_quoting=dbt_quoting,
        slack_webhook=slack_webhook,
        slack_token=slack_token,
        slack_channel_name=slack_channel_name,
        timezone=timezone,
        env=env,
        slack_group_alerts_by=group_by,
        report_url=report_url,
    )
    anonymous_tracking = AnonymousCommandLineTracking(config)
    anonymous_tracking.set_env("use_select", bool(select))
    try:
        config.validate_monitor()
        data_monitoring = E2EDataMonitoringAlerts(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=update_dbt_package,
            send_test_message_on_success=test,
            disable_samples=disable_samples,
            filter=select,
            global_suppression_interval=suppression_interval,
            override_config=override_dbt_project_config,
        )
        # The call to track_cli_start must be after the constructor of DataMonitoringAlerts as it enriches the tracking
        # properties. This is a tech-debt that should be fixed in the future.
        anonymous_tracking.track_cli_start(
            Command.MONITOR, get_cli_properties(), ctx.command.name
        )
        success = data_monitoring.run_alerts(
            days_back, full_refresh_dbt_package, dbt_vars=vars
        )
        anonymous_tracking.track_cli_end(
            Command.MONITOR, data_monitoring.properties(), ctx.command.name
        )
        if not success:
            sys.exit(1)
    except Exception as exc:
        anonymous_tracking.track_cli_exception(Command.MONITOR, exc, ctx.command.name)
        raise

    validation_passed = data_monitoring.validate_send_alerts()
    if not validation_passed:
        raise Exception("Validation failed")
