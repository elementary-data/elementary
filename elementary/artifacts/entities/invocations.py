import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    "invocation_id",
    "run_started_at",
    "command",
    "orchestrator",
    "target_name",
    "target_schema",
    "threads",
    "full_refresh",
]


@click.command("invocations")
@common_options
@click.option("--command", "command_arg", default=None, help="Filter by dbt command (e.g. 'build', 'run', 'test').")
@click.option("--project-name", default=None, help="Filter by dbt project name.")
@click.option(
    "--orchestrator",
    default=None,
    help="Filter by orchestrator (e.g. 'github_actions', 'airflow').",
)
@click.option(
    "--job-id",
    default=None,
    help="Filter by orchestrator job ID (NOT dbt invocation ID — use --invocation-id for that).",
)
@click.option("--job-run-id", default=None, help="Filter by orchestrator job run ID.")
@click.option(
    "--invocation-id",
    "invocation_ids",
    multiple=True,
    help="Filter by dbt invocation ID. Repeatable.",
)
@click.option("--target-name", default=None, help="Filter by target name (case-insensitive exact).")
@click.option("--target-schema", default=None, help="Filter by target schema (case-insensitive exact).")
@click.option(
    "--target-profile-name",
    default=None,
    help="Filter by target profile name (case-insensitive exact).",
)
@click.option(
    "--full-refresh/--no-full-refresh",
    "full_refresh",
    default=None,
    help="Filter by full_refresh flag.",
)
@click.option(
    "--started-after",
    default=None,
    help="Filter: run_started_at >= (ISO 8601). Defaults to 7 days ago.",
)
@click.option(
    "--started-before",
    default=None,
    help="Filter: run_started_at <= (ISO 8601). Defaults to now.",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 1000),
    default=200,
    help="Maximum number of invocations to return (default 200, max 1000).",
)
def invocations(
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
    command_arg,
    project_name,
    orchestrator,
    job_id,
    job_run_id,
    invocation_ids,
    target_name,
    target_schema,
    target_profile_name,
    full_refresh,
    started_after,
    started_before,
    limit,
):
    """List dbt invocations. Returns command, orchestrator, target, timing."""
    try:
        started_after_iso, started_before_iso = _resolve_time_window(
            started_after, started_before
        )
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_invocations",
            {
                "command": command_arg,
                "project_name": project_name,
                "orchestrator": orchestrator,
                "job_id": job_id,
                "job_run_id": job_run_id,
                "invocation_ids": list(invocation_ids) if invocation_ids else None,
                "target_name": target_name,
                "target_schema": target_schema,
                "target_profile_name": target_profile_name,
                "full_refresh": full_refresh,
                "started_after": started_after_iso,
                "started_before": started_before_iso,
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


@click.command("invocation")
@common_options
@click.argument("invocation_id")
def invocation(
    invocation_id,
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
):
    """Get a single dbt invocation by its ID."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_invocation",
            {"invocation_id": invocation_id},
        ) or []
        if not rows:
            sys.exit(
                emit_error(
                    f"Invocation {invocation_id} not found.",
                    ErrorCode.NOT_FOUND,
                    {"invocation_id": invocation_id},
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


def _resolve_time_window(
    started_after: Optional[str], started_before: Optional[str]
) -> Tuple[str, str]:
    now = datetime.now(timezone.utc)
    if started_after is None:
        started_after = (now - timedelta(days=7)).isoformat()
    if started_before is None:
        started_before = now.isoformat()
    return started_after, started_before


def _emit_list(rows: List[Dict[str, Any]], has_more: bool, output: str) -> None:
    if output == "table":
        emit_table(rows, LIST_COLUMNS)
        return
    emit_json(
        {
            "count": len(rows),
            "has_more": has_more,
            "invocations": rows,
            "data": {"length": len(rows)},
        }
    )


def _emit_single(row: Dict[str, Any], output: str) -> None:
    if output == "table":
        emit_table([row], LIST_COLUMNS)
        return
    emit_json({"invocation": row})
