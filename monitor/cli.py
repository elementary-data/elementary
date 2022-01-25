import click
import os
from os.path import expanduser
from utils.ordered_yaml import OrderedYaml
from config.config import Config
from monitor.data_monitoring import DataMonitoring

yaml = OrderedYaml()


@click.command()
@click.option(
    '--config-dir', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr')
)
@click.option(
    '--profiles-dir', '-p',
    type=str,
    default=os.path.join(expanduser('~'), '.dbt')
)
@click.option(
    '--update-dbt-package', '-u',
    type=bool,
    default=False
)
@click.option(
    '--full-refresh-dbt-package', '-f',
    type=bool,
    default=False
)
@click.option(
    '--reload-monitoring-configuration', '-r',
    type=bool,
    default=False
)
def monitor(config_dir, profiles_dir, update_dbt_package, full_refresh_dbt_package,
            reload_monitoring_configuration):
    click.echo(f"Any feedback and suggestions are welcomed! join our community here - "
               f"https://bit.ly/slack-elementary\n")
    data_monitoring = DataMonitoring.create_data_monitoring(config_dir, profiles_dir)
    data_monitoring.run(update_dbt_package, reload_monitoring_configuration, full_refresh_dbt_package)


if __name__ == "__main__":
    monitor()
