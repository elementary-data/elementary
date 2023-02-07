import sys

import click

from elementary.config.config import Config
from elementary.monitor.data_monitoring.data_monitoring_alerts import (
    DataMonitoringAlerts,
)
from elementary.monitor.data_monitoring.report.data_monitoring_report import (
    DataMonitoringReport,
)
from elementary.tracking.anonymous_tracking import AnonymousTracking
from elementary.utils import bucket_path
from elementary.utils.log import get_logger
from elementary.utils.ordered_yaml import OrderedYaml

yaml = OrderedYaml()

logger = get_logger(__name__)


class Command:
    MONITOR = "monitor"
    REPORT = "monitor-report"
    SEND_REPORT = "monitor-send-report"


# Displayed in reverse order in --help.
def common_options(cmd: str):
    def decorator(func):
        func = click.option(
            "--disable-samples",
            type=bool,
            default=False,
            help="Disable sampling of data. Useful if your data contains PII.",
        )(func)
        func = click.option(
            "--dbt-quoting",
            "-dq",
            type=str,
            default=None,
            help="Use this variable to override dbt's default quoting behavior for the edr internal dbt package. Can be "
            "one of the following: (1) all, (2) none, or (3) a combination of database,schema,identifier - for "
            'example "schema,identifier"',
        )(func)
        func = click.option(
            "--update-dbt-package",
            "-u",
            type=bool,
            default=False,
            help="Force downloading the latest version of the edr internal dbt package (usually this is not needed, "
            "see documentation to learn more).",
        )(func)
        if cmd in (Command.MONITOR, Command.SEND_REPORT):
            func = click.option(
                "--slack-channel-name",
                "-ch",
                type=str,
                default=None,
                help="The Slack channel to send messages to.",
            )(func)
            func = click.option(
                "--slack-token",
                "-st",
                type=str,
                default=None,
                help="The Slack token for your workspace.",
            )(func)
        if cmd in (Command.REPORT, Command.SEND_REPORT):
            func = click.option(
                "--exclude-elementary-models",
                type=bool,
                default=True,
                help="Exclude Elementary's internal models from the report.",
            )(func)
            func = click.option(
                "--project-name",
                type=str,
                default=None,
                help="The project name to display in the report.",
            )(func)
        func = click.option(
            "--days-back",
            "-d",
            type=int,
            default=1 if cmd == Command.MONITOR else 7,
            help="Set a limit to how far back should edr collect data.",
        )(func)
        func = click.option(
            "--env",
            type=click.Choice(["dev", "prod"]),
            default="dev",
            help="This flag indicates if you are running Elementary in prod or dev environment and will be reflected accordingly in the report.",
        )(func)
        func = click.option(
            "--config-dir",
            "-c",
            type=click.Path(),
            default=Config.DEFAULT_CONFIG_DIR,
            help="Global settings for edr are configured in a config.yml file in this directory "
            "(if your config dir is ~/.edr, no need to provide this parameter as we use it as default).",
        )(func)
        func = click.option(
            "--profile-target",
            "-t",
            type=str,
            default=None,
            help="Which target to load for the given profile. "
            "If specified, the target will be used for both the 'elementary' profile and your dbt project."
            "Else, the default target will be used.",
        )(func)
        func = click.option(
            "--profiles-dir",
            "-p",
            type=click.Path(exists=True),
            default=None,
            help="Which directory to look in for the profiles.yml file. "
            "If not set, edr will look in the current working directory first, then HOME/.dbt/",
        )(func)
        func = click.option(
            "--project-dir",
            type=click.Path(exists=True),
            default=None,
            help="Which directory to look in for the dbt_project.yml file. Default is the current working directory.",
        )(func)
        func = click.option(
            "--project-profile-target",
            type=str,
            default=None,
            help="Which target to load for the given profile. "
            "If specified, the target will be used for your dbt project."
            "Else, the --profile-target will be used.",
        )(func)
        func = click.option(
            "--select",
            type=str,
            default=None,
            help="Filter the report by tag:<TAG> / owner:<OWNER> / model:<MODEL> / last_invocation / invocation_id:<INVOCATION_ID> / invocation_time:<INVOCATION_TIME>."
            if cmd in (Command.REPORT, Command.SEND_REPORT)
            else "Filter the alerts by tag:<TAG> / owner:<OWNER> / model:<MODEL>.",
        )(func)
        return func

    return decorator


def get_cli_properties() -> dict:
    click_context = click.get_current_context()
    if click_context is None:
        return dict()

    params = click_context.params
    if params is None:
        return dict()

    reload_monitoring_configuration = params.get("reload_monitoring_configuration")
    update_dbt_package = params.get("update_dbt_package")
    full_refresh_dbt_package = params.get("full_refresh_dbt_package")

    return {
        "reload_monitoring_configuration": reload_monitoring_configuration,
        "update_dbt_package": update_dbt_package,
        "full_refresh_dbt_package": full_refresh_dbt_package,
    }


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
@click.pass_context
def monitor(
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
):
    """
    Get alerts on failures in dbt jobs.
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
        config_dir,
        profiles_dir,
        project_dir,
        profile_target,
        project_profile_target,
        dbt_quoting=dbt_quoting,
        slack_webhook=slack_webhook,
        slack_token=slack_token,
        slack_channel_name=slack_channel_name,
        timezone=timezone,
        env=env,
    )
    anonymous_tracking = AnonymousTracking(config)
    anonymous_tracking.set_env("use_select", bool(select))
    anonymous_tracking.track_cli_start(
        Command.MONITOR, get_cli_properties(), ctx.command.name
    )
    try:
        config.validate_monitor()
        data_monitoring = DataMonitoringAlerts(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=update_dbt_package,
            send_test_message_on_success=test,
            disable_samples=disable_samples,
            filter=select,
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


@monitor.command()
@common_options(Command.REPORT)
@click.option(
    "--executions-limit",
    "-el",
    type=int,
    default=720,
    help='Set the number of invocations shown for each test in the "Test Runs" report.',
)
@click.option(
    "--file-path",
    type=str,
    help="The file path where Elementary's report will be saved.",
)
@click.option(
    "--disable-passed-test-metrics",
    type=bool,
    default=False,
    help="If set to true elementary report won't show data metrics for passed tests (this can improve report creation time).",
)
@click.option(
    "--open-browser",
    type=bool,
    default=True,
    help="Whether to open the report in the browser.",
)
@click.pass_context
def report(
    ctx,
    days_back,
    config_dir,
    profiles_dir,
    project_dir,
    update_dbt_package,
    dbt_quoting,
    profile_target,
    project_profile_target,
    executions_limit,
    file_path,
    disable_passed_test_metrics,
    open_browser,
    exclude_elementary_models,
    disable_samples,
    project_name,
    env,
    select,
):
    """
    Generate a local observability report of your warehouse.
    """
    config = Config(
        config_dir,
        profiles_dir,
        project_dir,
        profile_target,
        project_profile_target,
        dbt_quoting=dbt_quoting,
        env=env,
    )
    anonymous_tracking = AnonymousTracking(config)
    anonymous_tracking.set_env("use_select", bool(select))
    anonymous_tracking.track_cli_start(
        Command.REPORT, get_cli_properties(), ctx.command.name
    )
    try:
        config.validate_report()
        data_monitoring = DataMonitoringReport(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=update_dbt_package,
            disable_samples=disable_samples,
            filter=select,
        )
        generated_report_successfully, _ = data_monitoring.generate_report(
            days_back=days_back,
            test_runs_amount=executions_limit,
            file_path=file_path,
            disable_passed_test_metrics=disable_passed_test_metrics,
            exclude_elementary_models=exclude_elementary_models,
            should_open_browser=open_browser,
            project_name=project_name,
        )
        anonymous_tracking.track_cli_end(
            Command.REPORT, data_monitoring.properties(), ctx.command.name
        )
        if not generated_report_successfully:
            sys.exit(1)
    except Exception as exc:
        anonymous_tracking.track_cli_exception(Command.REPORT, exc, ctx.command.name)
        raise


@monitor.command()
@common_options(Command.SEND_REPORT)
@click.option(
    "--slack-file-name",
    type=str,
    default=None,
    help="The report's file name, this is how it will be sent to slack.",
)
@click.option(
    "--aws-profile-name",
    type=str,
    default=None,
    help="AWS profile name",
)
@click.option(
    "--aws-access-key-id", type=str, default=None, help="The access key ID for AWS"
)
@click.option(
    "--aws-secret-access-key",
    type=str,
    default=None,
    help="The secret access key for AWS",
)
@click.option(
    "--s3-endpoint-url",
    type=str,
    default=None,
    help="The endpoint URL of the S3 bucket to upload the report to.",
)
@click.option(
    "--s3-bucket-name",
    type=str,
    default=None,
    help="The name of the S3 bucket to upload the report to.",
)
@click.option(
    "--google-service-account-path",
    type=str,
    default=None,
    help="The path to the Google service account JSON file",
)
@click.option(
    "--google-project-name",
    type=str,
    default=None,
    help="The GCloud project to upload the report to, otherwise use your default project.",
)
@click.option(
    "--gcs-bucket-name",
    type=str,
    default=None,
    help="The name of the GCS bucket to upload the report to.",
)
@click.option(
    "--update-bucket-website",
    type=bool,
    default=False,
    help="Update the bucket's static website with the latest report.",
)
@click.option(
    "--executions-limit",
    "-el",
    type=int,
    default=720,
    help='Set the number of invocations shown for each test in the "Test Runs" report.',
)
@click.option(
    "--bucket-file-path",
    type=str,
    default=None,
    help="The report's file name, this is where it will be stored in the bucket (may contain folders).",
)
@click.option(
    "--disable-passed-test-metrics",
    type=bool,
    default=False,
    help="If set to true elementary report won't show data metrics for passed tests (this can improve report creation time).",
)
@click.option(
    "--disable",
    type=str,
    default=None,
    help='Disable functualities from the "send-report" command.\nCurrently only --disable html_attachment is supported.',
)
@click.option(
    "--include",
    type=str,
    default=None,
    help="Include additional information at the test results summary message.\nCurrently only --include descriptions is supported.",
)
@click.pass_context
def send_report(
    ctx,
    days_back,
    config_dir,
    profiles_dir,
    project_dir,
    update_dbt_package,
    dbt_quoting,
    slack_token,
    slack_channel_name,
    slack_file_name,
    profile_target,
    project_profile_target,
    executions_limit,
    bucket_file_path,
    disable_passed_test_metrics,
    update_bucket_website,
    aws_profile_name,
    aws_access_key_id,
    aws_secret_access_key,
    s3_endpoint_url,
    s3_bucket_name,
    google_service_account_path,
    google_project_name,
    gcs_bucket_name,
    exclude_elementary_models,
    disable_samples,
    project_name,
    env,
    select,
    disable,
    include,
):
    """
    Generate and send the report to an external platform.
    The current options are Slack, AWS S3, and Google Cloud Storage.
    Each specified platform will be sent a report.
    """
    config = Config(
        config_dir,
        profiles_dir,
        project_dir,
        profile_target,
        project_profile_target,
        dbt_quoting=dbt_quoting,
        slack_token=slack_token,
        slack_channel_name=slack_channel_name,
        update_bucket_website=update_bucket_website,
        aws_profile_name=aws_profile_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        s3_endpoint_url=s3_endpoint_url,
        s3_bucket_name=s3_bucket_name,
        google_service_account_path=google_service_account_path,
        google_project_name=google_project_name,
        gcs_bucket_name=gcs_bucket_name,
        env=env,
    )
    anonymous_tracking = AnonymousTracking(config)
    anonymous_tracking.set_env("use_select", bool(select))
    anonymous_tracking.track_cli_start(
        Command.SEND_REPORT, get_cli_properties(), ctx.command.name
    )
    try:
        config.validate_send_report()
        # bucket-file-path determines the path of the report in the bucket.
        # If this path contains folders we extract the report file name to first save the report locally
        local_file_path = (
            bucket_path.basename(bucket_file_path)
            if bucket_file_path
            else slack_file_name
        )
        data_monitoring = DataMonitoringReport(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=update_dbt_package,
            disable_samples=disable_samples,
            filter=select,
        )
        sent_report_successfully = data_monitoring.send_report(
            days_back=days_back,
            test_runs_amount=executions_limit,
            disable_passed_test_metrics=disable_passed_test_metrics,
            file_path=local_file_path,
            should_open_browser=False,
            exclude_elementary_models=exclude_elementary_models,
            project_name=project_name,
            remote_file_path=bucket_file_path,
            disable_html_attachment=(disable == "html_attachment"),
            include_description=(include == "description"),
        )

        anonymous_tracking.track_cli_end(
            Command.SEND_REPORT, data_monitoring.properties(), ctx.command.name
        )

        if not sent_report_successfully:
            sys.exit(1)

    except Exception as exc:
        anonymous_tracking.track_cli_exception(
            Command.SEND_REPORT, exc, ctx.command.name
        )
        raise


if __name__ == "__main__":
    monitor()
