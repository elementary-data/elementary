from elementary.config.config import Config
from elementary.monitor.alerts.group_of_alerts import GroupingType
from tests.mocks.anonymous_tracking_mock import MockAnonymousTracking


def test_alerts():
    # very similar to the CLI, only mocking
    # - the part where alerts are updated as sent/skipped,
    # - the part that actually sends alerts to slack
    # instead assert they're as expected.
    # vars = yaml.loads(dbt_vars) if dbt_vars else None

    # TODO --warehouse-target from the pytest CLI into profile target here.
    config = Config(
        config_dir=Config.DEFAULT_CONFIG_DIR,
        profiles_dir=None,
        project_dir=None,
        profile_target="Snowflake",
        project_profile_target="Snowflake",
        dbt_quoting=None,
        slack_webhook=None,
        slack_token=None,
        slack_channel_name=None,
        timezone=None,
        env="dev",
        slack_group_alerts_by=GroupingType.BY_ALERT,
    )
    anonymous_tracking = MockAnonymousTracking(config)
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



    """
    Real Alerts fetcher.
    Mock Update as sent / Update as skipped, to sit nicely in the pipeline. 
    runs a similar data monitoring alerts, but no mark_alerts_as_read
    
    
    :return: 
    """"""
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
    group_by,
):
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
        slack_group_alerts_by=group_by,
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
