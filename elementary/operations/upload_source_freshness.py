import json
from pathlib import Path

import click

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.utils.ordered_yaml import OrderedYaml


def main(config: Config):
    if not config.project_dir:
        raise click.ClickException(
            "Could not find a dbt project in the current directory or --project-dir wasn't supplied."
        )
    project_dir = Path(config.project_dir)
    project_yml = OrderedYaml().load(str(project_dir.joinpath("dbt_project.yml")))
    target_path = project_dir / project_yml.get("target-path", "target")
    source_path = target_path / "sources.json"
    if not source_path.exists():
        raise click.ClickException(
            f"Could not find sources.json at {source_path}. "
            "Please run `dbt source freshness` before running this command."
        )
    results = json.loads(source_path.read_text())["results"]
    dbt_runner = DbtRunner(
        dbt_project_utils.PATH,
        config.profiles_dir,
        config.profile_target,
    )
    dbt_runner.run_operation(
        "elementary.upload_source_freshness",
        macro_args={"results": json.dumps(results)},
        quiet=True,
    )
    click.echo("Uploaded source freshness results successfully.")
