import sys

import click

from elementary.artifacts.entities import invocations as invocations_cmds
from elementary.artifacts.entities import models as models_cmds
from elementary.artifacts.entities import run_results as run_results_cmds
from elementary.artifacts.entities import sources as sources_cmds
from elementary.artifacts.entities import test_results as test_results_cmds
from elementary.artifacts.entities import tests as tests_cmds
from elementary.artifacts.fetching import ArtifactFetchError
from elementary.artifacts.output import ErrorCode, emit_error


@click.group("artifacts")
def artifacts():
    """Query Elementary's artifact tables for agent and script consumption.

    Every subcommand emits JSON to stdout by default (use `-o table` for
    human-readable output). Errors are written to stderr as JSON with a
    stable `{error, code, details}` shape. Exit code 0 on success, 1 on
    user error, 2 on system error.
    """
    pass


def _handle_fetch_error(exc: ArtifactFetchError) -> None:
    code = emit_error(str(exc), exc.code, exc.details)
    sys.exit(code)


def _handle_bad_argument(message: str, details: dict = None) -> None:
    code = emit_error(message, ErrorCode.BAD_ARGUMENT, details or {})
    sys.exit(code)


artifacts.add_command(test_results_cmds.test_results)
artifacts.add_command(test_results_cmds.test_result)
artifacts.add_command(run_results_cmds.run_results)
artifacts.add_command(run_results_cmds.run_result)
artifacts.add_command(invocations_cmds.invocations)
artifacts.add_command(invocations_cmds.invocation)
artifacts.add_command(models_cmds.models)
artifacts.add_command(models_cmds.model)
artifacts.add_command(sources_cmds.sources)
artifacts.add_command(sources_cmds.source)
artifacts.add_command(tests_cmds.tests)
artifacts.add_command(tests_cmds.test)
