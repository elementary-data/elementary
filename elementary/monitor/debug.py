import click

from elementary.clients.dbt.factory import create_dbt_runner_from_config
from elementary.config.config import Config
from elementary.exceptions.exceptions import DbtCommandError
from elementary.monitor.dbt_project_utils import CLI_DBT_PROJECT_PATH


class Debug:
    def __init__(self, config: Config):
        self.config = config

    def run(self) -> bool:
        dbt_runner = create_dbt_runner_from_config(self.config, CLI_DBT_PROJECT_PATH)

        try:
            dbt_runner.run_operation("elementary_cli.test_conn", quiet=True)
        except DbtCommandError as err:
            logs = (
                "\n".join(str(log) for log in err.logs)
                if err.logs
                else "No logs available"
            )
            click.echo(
                f"Could not connect to the Elementary db and schema. See details below\n\n{logs}"
            )
            return False

        click.echo("Connected to the Elementary db and schema successfully")
        return True
