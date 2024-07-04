import json
import os
from pathlib import Path

import click
from alive_progress import alive_it

from elementary.clients.dbt.factory import create_dbt_runner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.utils.ordered_yaml import OrderedYaml


class UploadSourceFreshnessOperation:
    def __init__(self, config: Config):
        self.config = config

    def run(self, rows_per_insert: int):
        if not self.config.project_dir:
            raise click.ClickException(
                "Path to dbt project is missing. Please run the command with `--project-dir <DBT_PROJECT_DIR>`."
            )
        sources_file_contents = self.get_sources_file_contents()
        results = sources_file_contents["results"]
        metadata = sources_file_contents["metadata"]
        self.upload_results(results, metadata, rows_per_insert)
        click.echo("Uploaded source freshness results successfully.")

    def get_sources_file_contents(self) -> dict:
        source_path = self.get_target_path() / "sources.json"
        if not source_path.exists():
            raise click.ClickException(
                f"Could not find sources.json at {source_path}. "
                "Please run `dbt source freshness` before running this command."
            )
        return json.loads(source_path.read_text())

    def upload_results(self, results: dict, metadata: dict, rows_per_insert: int):
        dbt_runner = create_dbt_runner(
            dbt_project_utils.CLI_DBT_PROJECT_PATH,
            self.config.profiles_dir,
            self.config.profile_target,
            run_deps_if_needed=self.config.run_dbt_deps_if_needed,
        )

        invocation_id = metadata.get("invocation_id")
        if not invocation_id:
            raise click.ClickException("No invocation id found in sources.json.")

        response = dbt_runner.run_operation(
            "elementary_cli.can_upload_source_freshness",
            macro_args={"invocation_id": invocation_id},
            quiet=True,
        )
        if not response:
            raise click.ClickException(
                f"Source freshness for invocation id {invocation_id} were already uploaded."
            )

        chunk_size = rows_per_insert
        chunk_list = list(range(0, len(results), chunk_size))
        upload_with_progress_bar = alive_it(
            chunk_list, title="Uploading source freshness results"
        )
        for chunk in upload_with_progress_bar:
            results_segment = results[chunk : chunk + chunk_size]

            for result in results_segment:
                result["metadata"] = metadata

            dbt_runner.run_operation(
                "elementary_cli.upload_source_freshness",
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
