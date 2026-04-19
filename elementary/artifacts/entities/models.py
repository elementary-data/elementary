import sys
from typing import Any, Dict, List

import click

from elementary.artifacts.common import build_config, common_options
from elementary.artifacts.fetching import (
    ArtifactFetchError,
    apply_pagination,
    parse_json_field,
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
    "unique_id",
    "name",
    "materialization",
    "database_name",
    "schema_name",
    "package_name",
    "group_name",
]

JSON_FIELDS = ("depends_on_nodes", "tags", "owner")


@click.command("models")
@common_options
@click.option("--database-name", default=None, help="Filter by database (case-insensitive exact).")
@click.option("--schema-name", default=None, help="Filter by schema (case-insensitive exact).")
@click.option(
    "--materialization",
    default=None,
    help="Filter by materialization (e.g. 'table', 'view', 'incremental').",
)
@click.option(
    "--name",
    default=None,
    help="Search by model name or alias (case-insensitive LIKE).",
)
@click.option("--package-name", default=None, help="Filter by dbt package (case-insensitive exact).")
@click.option("--group-name", default=None, help="Filter by group (case-insensitive exact).")
@click.option(
    "--generated-after",
    default=None,
    help="Filter: generated_at >= (ISO 8601 format).",
)
@click.option(
    "--generated-before",
    default=None,
    help="Filter: generated_at <= (ISO 8601 format).",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 1000),
    default=200,
    help="Maximum number of models to return (default 200, max 1000).",
)
def models(
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
    database_name,
    schema_name,
    materialization,
    name,
    package_name,
    group_name,
    generated_after,
    generated_before,
    limit,
):
    """List dbt models. Returns unique_id, materialization, schema, package, dependencies."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_models",
            {
                "database_name": database_name,
                "schema_name": schema_name,
                "materialization": materialization,
                "name": name,
                "package_name": package_name,
                "group_name": group_name,
                "generated_after": generated_after,
                "generated_before": generated_before,
                "limit": limit + 1,
            },
        ) or []
        trimmed, has_more = apply_pagination(rows, limit)
        _hydrate_json_fields(trimmed)
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


@click.command("model")
@common_options
@click.argument("unique_id")
def model(
    unique_id,
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
):
    """Get a single dbt model by its unique_id."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_model",
            {"unique_id": unique_id},
        ) or []
        if not rows:
            sys.exit(
                emit_error(
                    f"Model {unique_id} not found.",
                    ErrorCode.NOT_FOUND,
                    {"unique_id": unique_id},
                )
            )
        _hydrate_json_fields(rows)
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


def _hydrate_json_fields(rows: List[Dict[str, Any]]) -> None:
    for row in rows:
        for field in JSON_FIELDS:
            if field in row:
                row[field] = parse_json_field(row[field])


def _emit_list(rows: List[Dict[str, Any]], has_more: bool, output: str) -> None:
    if output == "table":
        emit_table(rows, LIST_COLUMNS)
        return
    emit_json(
        {
            "count": len(rows),
            "has_more": has_more,
            "models": rows,
            "data": {"length": len(rows)},
        }
    )


def _emit_single(row: Dict[str, Any], output: str) -> None:
    if output == "table":
        emit_table([row], LIST_COLUMNS)
        return
    emit_json({"model": row})
