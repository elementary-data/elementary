import click
import os
from os.path import expanduser
from utils.ordered_yaml import OrderedYaml
from observability.config import Config
from observability.data_monitoring import DataMonitoring

yaml = OrderedYaml()


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def observability(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = debug


@observability.command()
@click.pass_context
@click.option(
    '--config-dir-path', '-c',
    type=str,
    default=os.path.join(expanduser('~'), '.edr')
)
@click.option(
    '--profiles-dir-path', '-p',
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
def run(ctx, config_dir_path, profiles_dir_path, update_dbt_package, full_refresh_dbt_package,
        reload_monitoring_configuration):
    config = Config(config_dir_path, profiles_dir_path)
    data_monitoring = DataMonitoring.create_data_monitoring(config)
    data_monitoring.run(update_dbt_package, reload_monitoring_configuration, full_refresh_dbt_package)


if __name__ == "__main__":
    observability()
