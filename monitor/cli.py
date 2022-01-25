import click
import os
from os.path import expanduser
from utils.ordered_yaml import OrderedYaml
from config.config import Config
from monitor.data_monitoring import DataMonitoring

yaml = OrderedYaml()


@click.command()
@click.option(
    '--reload-monitoring-configuration', '-r',
    type=bool,
    default=False,
    help="edr automatically uploads the monitoring configuration if it does not exist in your data warehouse, "
         "use this flag if you changed your configuration and want to reload it."
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
def monitor(config_dir, profiles_dir, update_dbt_package, full_refresh_dbt_package,
            reload_monitoring_configuration):
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")
    data_monitoring = DataMonitoring.create_data_monitoring(config_dir, profiles_dir)
    data_monitoring.run(update_dbt_package, reload_monitoring_configuration, full_refresh_dbt_package)


if __name__ == "__main__":
    monitor()
