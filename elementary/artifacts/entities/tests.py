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
    "short_name",
    "test_column_name",
    "severity",
    "type",
    "parent_model_unique_id",
    "package_name",
]

LIST_JSON_FIELDS = ("tags", "model_tags", "model_owners")

DETAIL_JSON_FIELDS = (
    "test_params",
    "tags",
    "model_tags",
    "model_owners",
    "meta",
    "depends_on_macros",
    "depends_on_nodes",
)


@click.command("tests")
@common_options
@click.option("--database-name", default=None, help="Filter by database (case-insensitive exact).")
@click.option("--schema-name", default=None, help="Filter by schema (case-insensitive exact).")
@click.option(
    "--name",
    default=None,
    help="Search by test name, short_name, or alias (case-insensitive LIKE).",
)
@click.option("--package-name", default=None, help="Filter by dbt package (case-insensitive exact).")
@click.option(
    "--test-type",
    type=click.Choice(["generic", "singular", "expectation"], case_sensitive=False),
    default=None,
    help="Filter by test type.",
)
@click.option(
    "--test-namespace",
    default=None,
    help="Filter by test namespace/package (e.g. 'elementary', 'dbt_expectations').",
)
@click.option(
    "--severity",
    type=click.Choice(["warn", "error"], case_sensitive=False),
    default=None,
    help="Filter by severity.",
)
@click.option(
    "--parent-model-unique-id",
    default=None,
    help="Filter by the unique_id of the tested model.",
)
@click.option("--quality-dimension", default=None, help="Filter by quality dimension (case-insensitive exact).")
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
    help="Maximum number of tests to return (default 200, max 1000).",
)
def tests(
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
    database_name,
    schema_name,
    name,
    package_name,
    test_type,
    test_namespace,
    severity,
    parent_model_unique_id,
    quality_dimension,
    group_name,
    generated_after,
    generated_before,
    limit,
):
    """List dbt test definitions. Returns unique_id, short_name, severity, type, parent model."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_tests",
            {
                "database_name": database_name,
                "schema_name": schema_name,
                "name": name,
                "package_name": package_name,
                "test_type": test_type,
                "test_namespace": test_namespace,
                "severity": severity,
                "parent_model_unique_id": parent_model_unique_id,
                "quality_dimension": quality_dimension,
                "group_name": group_name,
                "generated_after": generated_after,
                "generated_before": generated_before,
                "limit": limit + 1,
            },
        ) or []
        trimmed, has_more = apply_pagination(rows, limit)
        _hydrate_json_fields(trimmed, LIST_JSON_FIELDS)
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


@click.command("test")
@common_options
@click.argument("unique_id")
def test(
    unique_id,
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
):
    """Get a single dbt test definition by its unique_id."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_dbt_test",
            {"unique_id": unique_id},
        ) or []
        if not rows:
            sys.exit(
                emit_error(
                    f"Test {unique_id} not found.",
                    ErrorCode.NOT_FOUND,
                    {"unique_id": unique_id},
                )
            )
        _hydrate_json_fields(rows, DETAIL_JSON_FIELDS)
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


def _hydrate_json_fields(rows: List[Dict[str, Any]], fields) -> None:
    for row in rows:
        for field in fields:
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
            "tests": rows,
            "data": {"length": len(rows)},
        }
    )


def _emit_single(row: Dict[str, Any], output: str) -> None:
    if output == "table":
        emit_table([row], LIST_COLUMNS)
        return
    emit_json({"test": row})
