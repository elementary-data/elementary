import sys
from typing import Any, Dict, List, Optional

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
    "id",
    "detected_at",
    "status",
    "test_type",
    "test_name",
    "table_name",
    "column_name",
]


@click.command("test-results")
@common_options
@click.option("--test-unique-id", default=None, help="Filter by test unique_id (exact).")
@click.option(
    "--model-unique-id", default=None, help="Filter by model unique_id (exact)."
)
@click.option(
    "--test-type",
    default=None,
    help="Filter by test type (e.g. 'dbt_test', 'anomaly_detection', 'schema_change').",
)
@click.option(
    "--test-sub-type",
    default=None,
    help="Filter by test sub-type (exact, case-insensitive).",
)
@click.option(
    "--test-name", default=None, help="Search by test name (case-insensitive LIKE)."
)
@click.option(
    "--status",
    default=None,
    help="Filter by status (e.g. 'pass', 'fail', 'warn', 'error').",
)
@click.option(
    "--table-name", default=None, help="Search by table name (case-insensitive LIKE)."
)
@click.option(
    "--column-name", default=None, help="Filter by column name (exact, case-insensitive)."
)
@click.option(
    "--database-name",
    default=None,
    help="Filter by database name (exact, case-insensitive).",
)
@click.option(
    "--schema-name",
    default=None,
    help="Filter by schema name (exact, case-insensitive).",
)
@click.option("--severity", default=None, help="Filter by severity (e.g. 'WARN', 'ERROR').")
@click.option(
    "--detected-after",
    default=None,
    help="Filter: detected_at >= (ISO 8601 format).",
)
@click.option(
    "--detected-before",
    default=None,
    help="Filter: detected_at <= (ISO 8601 format).",
)
@click.option(
    "--limit",
    type=click.IntRange(1, 1000),
    default=200,
    help="Maximum number of test results to return (default 200, max 1000).",
)
def test_results(
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
    test_unique_id,
    model_unique_id,
    test_type,
    test_sub_type,
    test_name,
    status,
    table_name,
    column_name,
    database_name,
    schema_name,
    severity,
    detected_after,
    detected_before,
    limit,
):
    """List Elementary test results. Returns status, detected_at, test type, table/column."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_elementary_test_results",
            {
                "test_unique_id": test_unique_id,
                "model_unique_id": model_unique_id,
                "test_type": test_type,
                "test_sub_type": test_sub_type,
                "test_name": test_name,
                "status": status,
                "table_name": table_name,
                "column_name": column_name,
                "database_name": database_name,
                "schema_name": schema_name,
                "severity": severity,
                "detected_after": detected_after,
                "detected_before": detected_before,
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


@click.command("test-result")
@common_options
@click.argument("test_result_id")
def test_result(
    test_result_id,
    output,
    target_path,
    config_dir,
    profile_name,
    profile_target,
    profiles_dir,
    project_dir,
):
    """Get a single Elementary test result by its ID."""
    try:
        config = build_config(
            config_dir, profiles_dir, project_dir, profile_target, target_path
        )
        runner = create_artifacts_runner(config, profile=profile_name)
        rows = run_macro(
            runner,
            "elementary_cli.get_elementary_test_result",
            {"test_result_id": test_result_id},
        ) or []
        if not rows:
            sys.exit(
                emit_error(
                    f"Test result {test_result_id} not found.",
                    ErrorCode.NOT_FOUND,
                    {"test_result_id": test_result_id},
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
            "test_results": rows,
            "data": {"length": len(rows)},
        }
    )


def _emit_single(row: Dict[str, Any], output: str) -> None:
    if output == "table":
        emit_table([row], LIST_COLUMNS)
        return
    emit_json({"test_result": row})
