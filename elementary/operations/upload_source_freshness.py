import json
import os
from itertools import accumulate, groupby
from operator import itemgetter
from pathlib import Path

import click
from alive_progress import alive_it

from elementary.clients.dbt.dbt_runner import DbtRunner
from elementary.config.config import Config
from elementary.monitor import dbt_project_utils
from elementary.utils.ordered_yaml import OrderedYaml

MAX_SERIALISED_CHARACTER_LENGTH = 90_000


class UploadSourceFreshnessOperation:
    def __init__(self, config: Config):
        self.config = config

    def run(self):
        if not self.config.project_dir:
            raise click.ClickException(
                "Path to dbt project is missing. Please run the command with `--project-dir <DBT_PROJECT_DIR>`."
            )
        sources_file_contents = self.get_sources_file_contents()
        results = sources_file_contents["results"]
        metadata = sources_file_contents["metadata"]
        self.upload_results(results, metadata)
        click.echo("Uploaded source freshness results successfully.")

    def get_sources_file_contents(self) -> dict:
        source_path = self.get_target_path() / "sources.json"
        if not source_path.exists():
            raise click.ClickException(
                f"Could not find sources.json at {source_path}. "
                "Please run `dbt source freshness` before running this command."
            )
        return json.loads(source_path.read_text())

    def upload_results(self, results: dict, metadata: dict):
        dbt_runner = DbtRunner(
            dbt_project_utils.PATH,
            self.config.profiles_dir,
            self.config.profile_target,
        )
        if not dbt_project_utils.is_dbt_package_up_to_date():
            dbt_runner.deps()

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

        for result in results:
            result["metadata"] = metadata

        serialised_argument_lengths = [len(json.dumps(record)) for record in results]

        argument_splits = [
            (running_total // MAX_SERIALISED_CHARACTER_LENGTH, record)
            for running_total, record in zip(
                accumulate(serialised_argument_lengths),
                results,
            )
        ]

        chunked_commands = {
            chunk_id: [i[1] for i in chunk_tuple]
            for chunk_id, chunk_tuple in groupby(argument_splits, key=itemgetter(0))
        }

        upload_with_progress_bar = alive_it(
            chunked_commands.values(), title="Uploading source freshness results"
        )

        for result_chunk in upload_with_progress_bar:
            dbt_runner.run_operation(
                "elementary_cli.upload_source_freshness",
                macro_args={"results": json.dumps(result_chunk)},
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
