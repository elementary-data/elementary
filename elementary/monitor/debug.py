import click

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.exceptions.exceptions import DbtCommandError
from elementary.monitor import dbt_project_utils


class Debug:
    def __init__(self, config: Config):
        self.config = config

    def run(self) -> bool:
        dbt_runner = DbtRunner(
            dbt_project_utils.PATH,
            self.config.profiles_dir,
            self.config.profile_target,
        )

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
