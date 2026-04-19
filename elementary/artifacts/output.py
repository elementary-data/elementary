import json
import sys
from typing import Any, Dict, List, Optional, Sequence


class ErrorCode:
    BAD_ARGUMENT = "BAD_ARGUMENT"
    NOT_AUTHENTICATED = "NOT_AUTHENTICATED"
    NOT_FOUND = "NOT_FOUND"
    WAREHOUSE_ERROR = "WAREHOUSE_ERROR"
    MALFORMED_ARTIFACTS = "MALFORMED_ARTIFACTS"
    INTERNAL_ERROR = "INTERNAL_ERROR"


_USER_ERROR_CODES = {
    ErrorCode.BAD_ARGUMENT,
    ErrorCode.NOT_AUTHENTICATED,
    ErrorCode.NOT_FOUND,
}


def exit_code_for(code: str) -> int:
    return 1 if code in _USER_ERROR_CODES else 2


def emit_json(payload: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, default=str))
    sys.stdout.write("\n")
    sys.stdout.flush()


def emit_table(rows: Sequence[Dict[str, Any]], columns: Sequence[str]) -> None:
    if not rows:
        sys.stdout.write("(no results)\n")
        sys.stdout.flush()
        return
    widths = {c: len(c) for c in columns}
    stringified: List[Dict[str, str]] = []
    for row in rows:
        s = {c: _stringify(row.get(c)) for c in columns}
        stringified.append(s)
        for c in columns:
            widths[c] = max(widths[c], len(s[c]))
    header = "  ".join(c.ljust(widths[c]) for c in columns)
    sep = "  ".join("-" * widths[c] for c in columns)
    sys.stdout.write(header + "\n")
    sys.stdout.write(sep + "\n")
    for row in stringified:
        sys.stdout.write("  ".join(row[c].ljust(widths[c]) for c in columns) + "\n")
    sys.stdout.flush()


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)


def emit_error(
    message: str,
    code: str,
    details: Optional[Dict[str, Any]] = None,
) -> int:
    envelope: Dict[str, Any] = {"error": message, "code": code, "details": details or {}}
    sys.stderr.write(json.dumps(envelope, default=str))
    sys.stderr.write("\n")
    sys.stderr.flush()
    return exit_code_for(code)
