import json
from typing import Any, Dict, List, Optional, Tuple

from elementary.artifacts.output import ErrorCode
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.exceptions.exceptions import DbtCommandError


class ArtifactFetchError(Exception):
    def __init__(self, message: str, code: str, details: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


def run_macro(
    dbt_runner: BaseDbtRunner,
    macro_name: str,
    macro_args: Dict[str, Any],
) -> Any:
    """Run a macro in the internal dbt project, parse the JSON result.

    Raises ArtifactFetchError with a stable code on failure so callers can
    emit a consistent error envelope.
    """
    try:
        response = dbt_runner.run_operation(
            macro_name=macro_name,
            macro_args=macro_args,
            log_errors=True,
            quiet=True,
        )
    except DbtCommandError as exc:
        raise ArtifactFetchError(
            f"dbt command failed while running {macro_name}.",
            ErrorCode.WAREHOUSE_ERROR,
            {"dbt_error": str(exc)},
        ) from exc
    except Exception as exc:
        raise ArtifactFetchError(
            f"Unexpected failure running {macro_name}: {exc}",
            ErrorCode.INTERNAL_ERROR,
        ) from exc

    if not response:
        return None
    try:
        return json.loads(response[0])
    except (ValueError, IndexError) as exc:
        raise ArtifactFetchError(
            f"Malformed response from {macro_name}.",
            ErrorCode.MALFORMED_ARTIFACTS,
            {"raw": response[0] if response else None},
        ) from exc


def apply_pagination(rows: List[Any], limit: int) -> Tuple[List[Any], bool]:
    """Given rows fetched with `limit + 1`, trim to `limit` and report truncation."""
    has_more = len(rows) > limit
    return rows[:limit], has_more


def parse_json_field(value: Any) -> Any:
    """dbt returns list/dict columns as JSON-encoded strings. Parse them."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value
