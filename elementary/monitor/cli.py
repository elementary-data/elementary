import sys

import click

from elementary.config.config import Config
from elementary.monitor.data_monitoring.alerts.data_monitoring_alerts import (
    DataMonitoringAlerts,
)
from elementary.monitor.data_monitoring.report.data_monitoring_report import (
    DataMonitoringReport,
)
from elementary.monitor.data_monitoring.schema import FiltersSchema
from elementary.monitor.data_monitoring.selector_filter import SelectorFilter
from elementary.monitor.dbt_init import DBTInit
from elementary.monitor.debug import Debug
from elementary.tracking.anonymous_tracking import AnonymousCommandLineTracking
from elementary.utils import bucket_path
from elementary.utils.ordered_yaml import OrderedYaml

yaml = OrderedYaml()


class Command:
    MONITOR = "monitor"
    REPORT = "monitor-report"
    SEND_REPORT = "monitor-send-report"
    DEBUG = "debug"
    DBT_INIT = "dbt-init"


# Displayed in reverse order in --help.
def common_options(cmd: str):
    def decorator(func):
        func = click.option(
            "--target-path",
            type=str,
            default=Config.DEFAULT_TARGET_PATH,
            help="Absolute target path for saving edr files such as logs and reports",
        )(func)
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
            type=str,
            default="dev",
            help="This flag indicates which environment you are running Elementary in (e.g. dev or prod) and will be reflected accordingly in the report.",
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
            "If specified, the target will be used for both the 'elementary' profile and your dbt project. "
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
            "If specified, the target will be used for your dbt project. "
            "Else, the --profile-target will be used.",
        )(func)
        func = click.option(
            "--select",
            type=str,
            default=None,
            help="Filter the report by last_invocation / invocation_id:<INVOCATION_ID> / invocation_time:<INVOCATION_TIME>."
            if cmd in (Command.REPORT, Command.SEND_REPORT)
            else "DEPRECATED! Please use --filters instead! - Filter the alerts by tags:<TAGS> / owners:<OWNERS> / models:<MODELS> / "
            "statuses:<warn/fail/error/skipped> / resource_types:<model/test>.",
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

    target_path = params.get("target_path")
    update_dbt_package = params.get("update_dbt_package")
    full_refresh_dbt_package = params.get("full_refresh_dbt_package")
    select = params.get("select")
    days_back = params.get("days_back")
    timezone = params.get("timezone")
    group_by = params.get("group_by")
    suppression_interval = params.get("suppression_interval")
    override_dbt_project_config = params.get("override_dbt_project_config")

    return {
        "target_path": target_path,
        "update_dbt_package": update_dbt_package,
        "full_refresh_dbt_package": full_refresh_dbt_package,
        "select": select,
        "days_back": days_back,
        "timezone": timezone,
        "group_by": group_by,
        "suppression_interval": suppression_interval,
        "override_dbt_project_config": override_dbt_project_config,
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
    "--group-alerts-threshold",
    type=int,
    default=Config.DEFAULT_GROUP_ALERTS_THRESHOLD,
    help="The threshold for all alerts in a single message.",
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
@click.option(
    "--filters",
    "-fl",
    type=str,
    default=None,
    multiple=True,
    help="Filter the alerts by tags:<tags separated by commas> / owners:<owners separated by commas> / models:<models separated by commas> / "
    "statuses:<warn/fail/error/skipped> / resource_types:<model/test>.",
)
@click.option(
    "--teams-webhook",
    "-tw",
    type=str,
    default=None,
    help="A Microsoft Teams webhook URL for sending alerts to a specific channel in Teams.",
)
@click.pass_context
def monitor(
    ctx,
    days_back,
    slack_webhook,
    deprecated_slack_webhook,
    slack_token,
    slack_channel_name,
    group_alerts_threshold,
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
    filters,
    teams_webhook,
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
        group_alerts_threshold=group_alerts_threshold,
        timezone=timezone,
        env=env,
        slack_group_alerts_by=group_by,
        report_url=report_url,
        teams_webhook=teams_webhook,
    )
    anonymous_tracking = AnonymousCommandLineTracking(config)
    anonymous_tracking.set_env("use_select", bool(select))
    try:
        config.validate_monitor()

        alert_filters = FiltersSchema()
        if bool(filters):
            alert_filters = FiltersSchema.from_cli_params(filters)
        elif select is not None:
            click.secho(
                '\n"--select" is deprecated and won\'t be supported in the near future.\n'
                'Please use "-fl" or "--filter" for filtering the alerts.\n',
                fg="bright_red",
            )
            alert_filters = SelectorFilter(
                config, anonymous_tracking, select
            ).get_filter()

        data_monitoring = DataMonitoringAlerts(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=update_dbt_package,
            send_test_message_on_success=test,
            disable_samples=disable_samples,
            selector_filter=alert_filters,
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
    target_path,
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
        target_path,
        dbt_quoting=dbt_quoting,
        env=env,
    )
    anonymous_tracking = AnonymousCommandLineTracking(config)
    anonymous_tracking.set_env("use_select", bool(select))
    try:
        selector_filter = SelectorFilter(config, anonymous_tracking, select)
        data_monitoring = DataMonitoringReport(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=update_dbt_package,
            disable_samples=disable_samples,
            selector_filter=selector_filter.get_filter(),
        )
        data_monitoring.validate_report_selector()
        # The call to track_cli_start must be after the constructor of DataMonitoringAlerts as it enriches the tracking properties.
        # This is a tech-debt that should be fixed in the future.
        anonymous_tracking.track_cli_start(
            Command.REPORT, get_cli_properties(), ctx.command.name
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
    "--aws-region-name",
    type=str,
    default=None,
    help="AWS region name",
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
    "--aws-session-token",
    type=str,
    default=None,
    help="The session token for AWS",
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
    "--s3-acl",
    type=str,
    default=None,
    help="S3 Canned ACL value used to modify report permissions, for example set to 'public-read' to make the report publicly accessible.",
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
    "--gcs-timeout-limit",
    type=int,
    default=None,
    help="GCS requests timeout limit in seconds. If not provided the default is 60.",
)
@click.option(
    "--azure-connection-string",
    type=str,
    default=None,
    help="A connection string required to connect to your Azure storage account.",
)
@click.option(
    "--azure-container-name",
    type=str,
    default=None,
    help="The name of the Azure container to upload the report to.",
)
@click.option(
    "--update-bucket-website",
    type=bool,
    default=None,
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
    "--slack-report-url",
    type=str,
    default=None,
    help="DEPRECATED! - The URL for the report at the Slack summary message (if not provided edr will assume the default bucket website url).",
)
@click.option(
    "--report-url",
    type=str,
    default=None,
    help="The URL for the report at the Slack summary message (if not provided edr will assume the default bucket website url).",
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
    help='Disable functionalities from the "send-report" command.\nCurrently only --disable html_attachment is supported.',
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
    slack_report_url,
    report_url,
    disable_passed_test_metrics,
    update_bucket_website,
    aws_profile_name,
    aws_region_name,
    aws_access_key_id,
    aws_secret_access_key,
    aws_session_token,
    s3_endpoint_url,
    s3_bucket_name,
    s3_acl,
    azure_connection_string,
    azure_container_name,
    google_service_account_path,
    google_project_name,
    gcs_bucket_name,
    gcs_timeout_limit,
    exclude_elementary_models,
    disable_samples,
    project_name,
    env,
    select,
    disable,
    include,
    target_path,
):
    """
    Generate and send the report to an external platform.
    The current options are Slack, AWS S3, and Google Cloud Storage.
    Each specified platform will be sent a report.
    """
    if slack_report_url is not None:
        click.secho(
            '\n"--slack-report-url" is deprecated and won\'t be supported in the near future.\n'
            'Please use "--report-url" for passing report URL.\n',
            fg="bright_red",
        )
        report_url = slack_report_url

    config = Config(
        config_dir=config_dir,
        profiles_dir=profiles_dir,
        project_dir=project_dir,
        profile_target=profile_target,
        project_profile_target=project_profile_target,
        target_path=target_path,
        dbt_quoting=dbt_quoting,
        slack_token=slack_token,
        slack_channel_name=slack_channel_name,
        update_bucket_website=update_bucket_website,
        aws_profile_name=aws_profile_name,
        aws_region_name=aws_region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        azure_connection_string=azure_connection_string,
        azure_container_name=azure_container_name,
        s3_endpoint_url=s3_endpoint_url,
        s3_bucket_name=s3_bucket_name,
        s3_acl=s3_acl,
        google_service_account_path=google_service_account_path,
        google_project_name=google_project_name,
        gcs_bucket_name=gcs_bucket_name,
        gcs_timeout_limit=gcs_timeout_limit,
        report_url=report_url,
        env=env,
        project_name=project_name,
    )
    anonymous_tracking = AnonymousCommandLineTracking(config)
    anonymous_tracking.set_env("use_select", bool(select))
    try:
        config.validate_send_report()
        # bucket-file-path determines the path of the report in the bucket.
        # If this path contains folders we extract the report file name to first save the report locally
        local_file_path = (
            bucket_path.basename(bucket_file_path)
            if bucket_file_path
            else slack_file_name
        )
        selector_filter = SelectorFilter(config, anonymous_tracking, select)
        data_monitoring = DataMonitoringReport(
            config=config,
            tracking=anonymous_tracking,
            force_update_dbt_package=update_dbt_package,
            disable_samples=disable_samples,
            selector_filter=selector_filter.get_filter(),
        )
        # The call to track_cli_start must be after the constructor of DataMonitoringAlerts as it enriches the tracking properties.
        # This is a tech-debt that should be fixed in the future.
        anonymous_tracking.track_cli_start(
            Command.SEND_REPORT, get_cli_properties(), ctx.command.name
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


@monitor.command()
@click.option(
    "--profiles-dir",
    "-p",
    type=click.Path(exists=True),
    default=None,
    help="Which directory to look in for the profiles.yml file. "
    "If not set, edr will look in the current working directory first, then HOME/.dbt/",
)
@click.pass_context
def debug(ctx, profiles_dir):
    config = Config(profiles_dir=profiles_dir)
    anonymous_tracking = AnonymousCommandLineTracking(config)
    anonymous_tracking.track_cli_start(Command.DEBUG, None, ctx.command.name)
    success = Debug(config).run()
    if not success:
        sys.exit(1)

    anonymous_tracking.track_cli_end(Command.DEBUG, None, ctx.command.name)


@monitor.command()
@click.pass_context
def dbt_init(ctx):
    """
    Initializes the Elementary internal dbt project by installing its dbt deps.
    Run this command after installing EDR as part of builds or CI/CD pipelines when the target
    environment does not have write permissions on disk or does not have internet connection.
    This command is not needed in most cases as the dbt deps are installed automatically when running `edr monitor`.
    """
    config = Config()
    anonymous_tracking = AnonymousCommandLineTracking(config)
    anonymous_tracking.track_cli_start(Command.DEBUG, None, ctx.command.name)
    dbtinit = DBTInit()
    success = dbtinit.setup_internal_dbt_packages()
    if not success:
        sys.exit(1)
    click.echo("Elementary internal dbt project has been initialized successfully. ")
    anonymous_tracking.track_cli_end(Command.DEBUG, None, ctx.command.name)


if __name__ == "__main__":
    monitor()
