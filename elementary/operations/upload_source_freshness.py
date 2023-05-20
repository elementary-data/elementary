import json
import os
from pathlib import Path

import click
from alive_progress import alive_it

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.utils.ordered_yaml import OrderedYaml


class UploadSourceFreshnessOperation:
    def __init__(self, config: Config):
        self.config = config

    def run(self):
        if not self.config.project_dir:
            raise click.ClickException(
                "Path to dbt project is missing. Please run the command with `--project-dir <DBT_PROJECT_DIR>`."
            )
        results = self.get_results()
        self.upload_results(results)
        click.echo("Uploaded source freshness results successfully.")

    def get_results(self) -> dict:
        source_path = self.get_target_path() / "sources.json"
        if not source_path.exists():
            raise click.ClickException(
                f"Could not find sources.json at {source_path}. "
                "Please run `dbt source freshness` before running this command."
            )
        return json.loads(source_path.read_text())["results"]

    def upload_results(self, results: dict):
        dbt_runner = DbtRunner(
            dbt_project_utils.PATH,
            self.config.profiles_dir,
            self.config.profile_target,
        )
        chunk_size = 100
        chunk_list = list(range(0, len(results), chunk_size))
        upload_with_progress_bar = alive_it(
            chunk_list, title="Uploading source freshness results"
        )
        for chunk in upload_with_progress_bar:
            results_segment = results[chunk : chunk + chunk_size]
            dbt_runner.run_operation(
                "elementary_internal.upload_source_freshness",
                macro_args={"results": json.dumps(results_segment)},
                quiet=True,
            )

    def get_target_path(self) -> Path:
        if not self.config.project_dir:
            raise click.ClickException(
                "Path to dbt project is missing. Please run the command with `--project-dir <DBT_PROJECT_DIR>`."
            )

        env_target_path = os.getenv("DBT_TARGET_PATH")
        if env_target_path:
            return Path(env_target_path)
        project_dir = Path(self.config.project_dir)
        project_yml = OrderedYaml().load(str(project_dir.joinpath("dbt_project.yml")))
        return project_dir / project_yml.get("target-path", "target")
