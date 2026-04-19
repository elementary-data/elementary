import sys
from typing import Any, Dict, List

import click

from elementary.artifacts.common import build_config, common_options
from elementary.artifacts.fetching import (
    ArtifactFetchError,
    apply_pagination,
    run_macro,
)
from elementary.artifacts.output import (
    ErrorCode,
    emit_error,
    emit_json,
    emit_table,
)
from elementary.artifacts.runner import create_artifacts_runner

LIST_COLUMNS = [
    "model_execution_id",
    "execute_started_at",
    "status",
    "resource_type",
    "materialization",
    "execution_time",
    "name",
    "unique_id",
]


@click.command("run-results")
@common_options
@click.option("--unique-id", default=None, help="Filter by asset unique_id (exact).")
@click.option(
    "--invocation-id",
    default=None,
    help="Filter to a specific dbt invocation.",
)
@click.option(
    "--status",
    default=None,
    help="Filter by status (e.g. 'success', 'error', 'skipped', 'warn', 'fail', 'pass').",
)
@click.option(
    "--resource-type",
    default=None,
    help="Filter by resource type (e.g. 'model', 'test', 'snapshot', 'seed').",
)
@click.option(
    "--materialization",
    default=None,
    help="Filter by materialization (e.g. 'table', 'view', 'incremental').",
)
@click.option(
    "--name", default=None, help="Search by resource name (case-insensitive LIKE)."
)
@click.option(
    "--started-after",
    default=None,
    help="Filter: execute_started_at >= (ISO 8601 format).",
)
@click.option(
    "--started-before",
    default=None,
    help="Filter: execute_started_at <= (ISO 8601 format).",
)
@click.option(
    "--execution-time-gt",
    type=float,
    default=None,
    help="Filter: execution_time > value (seconds).",
)
@click.option(
    "--execution-time-lt",
    type=float,
    default=None,
    help="Filter: execution_time < value (seconds).",
)
@click.option(
    "--include-compiled-code",
    is_flag=True,
    default=False,
    help="Include compiled_code and adapter_response fields (large).",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 1000),
    default=200,
    help="Maximum number of run results to return (default 200, max 1000).",
)
def run_results(
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
    unique_id,
    invocation_id,
    status,
    resource_type,
    materialization,
    name,
    started_after,
    started_before,
    execution_time_gt,
    execution_time_lt,
    include_compiled_code,
    limit,
):
    """List dbt run results. Returns execution status, timing, materialization, details."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_run_results",
            {
                "unique_id": unique_id,
                "invocation_id": invocation_id,
                "status": status,
                "resource_type": resource_type,
                "materialization": materialization,
                "name": name,
                "started_after": started_after,
                "started_before": started_before,
                "execution_time_gt": execution_time_gt,
                "execution_time_lt": execution_time_lt,
                "lightweight": not include_compiled_code,
                "limit": limit + 1,
            },
        ) or []
        trimmed, has_more = apply_pagination(rows, limit)
        _emit_list(trimmed, has_more, output)
    except ArtifactFetchError as exc:
        sys.exit(emit_error(str(exc), exc.code, exc.details))
    except Exception as exc:
        sys.exit(
            emit_error(
                f"Unexpected error: {exc}",
                ErrorCode.INTERNAL_ERROR,
                {"type": type(exc).__name__},
            )
        )


@click.command("run-result")
@common_options
@click.option(
    "--include-compiled-code",
    is_flag=True,
    default=False,
    help="Include compiled_code and adapter_response fields (large).",
)
@click.argument("model_execution_id")
def run_result(
    model_execution_id,
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
    include_compiled_code,
):
    """Get a single dbt run result by its model_execution_id."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_run_result",
            {
                "model_execution_id": model_execution_id,
                "include_compiled_code": include_compiled_code,
            },
        ) or []
        if not rows:
            sys.exit(
                emit_error(
                    f"Run result {model_execution_id} not found.",
                    ErrorCode.NOT_FOUND,
                    {"model_execution_id": model_execution_id},
                )
            )
        _emit_single(rows[0], output)
    except ArtifactFetchError as exc:
        sys.exit(emit_error(str(exc), exc.code, exc.details))
    except Exception as exc:
        sys.exit(
            emit_error(
                f"Unexpected error: {exc}",
                ErrorCode.INTERNAL_ERROR,
                {"type": type(exc).__name__},
            )
        )


def _emit_list(rows: List[Dict[str, Any]], has_more: bool, output: str) -> None:
    if output == "table":
        emit_table(rows, LIST_COLUMNS)
        return
    emit_json(
        {
            "count": len(rows),
            "has_more": has_more,
            "run_results": rows,
            "data": {"length": len(rows)},
        }
    )


def _emit_single(row: Dict[str, Any], output: str) -> None:
    if output == "table":
        emit_table([row], LIST_COLUMNS)
        return
    emit_json({"run_result": row})
