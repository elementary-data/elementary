"""Per-adapter transient error patterns for automatic retry.

Each adapter may produce transient errors that are safe to retry.  This
module centralises those patterns so that the runner can decide whether a
failed dbt command should be retried transparently.

To add patterns for a new adapter, append a new entry to
``_ADAPTER_PATTERNS`` with the **adapter type** as key (e.g.
``"bigquery"``, ``"snowflake"``) and a tuple of **plain, lowercase**
substrings that appear in the error output.  Matching is
case-insensitive substring search so regex is not needed.

Note: The ``target`` argument accepted by :func:`is_transient_error` may
be either the dbt adapter type *or* the profile target name (e.g.
``"dev"``, ``"prod"``).  When it does not match any known adapter key,
**all** adapter patterns are checked defensively.  This is safe because
adapter-specific error messages only appear in output from that adapter.
"""

from typing import Dict, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Per-adapter transient error substrings (all lowercase).
#
# A command failure is considered *transient* when the dbt output
# (stdout + stderr, lowercased) contains **any** of the substrings
# listed for the active adapter **or** in the ``_COMMON`` list.
# ---------------------------------------------------------------------------

_COMMON: Tuple[str, ...] = (
    # Generic connection / HTTP errors that any adapter can surface.
    "connection reset by peer",
    "connection was closed",
    "remotedisconnected",
    "connectionerror",
    "brokenpipeerror",
    "connection aborted",
    "read timed out",
)

_DATABRICKS_PATTERNS: Tuple[str, ...] = (
    "temporarily_unavailable",
    "504 gateway timeout",
    "502 bad gateway",
    "service unavailable",
)

_ADAPTER_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "bigquery": (
        # Streaming-buffer delay after a streaming insert.
        "streaming data from",
        "is temporarily unavailable",
        # Generic transient backend error (500).
        "retrying may solve the problem",
        "backenderror",
        # Rate-limit / quota errors.
        "exceeded rate limits",
        "rateLimitExceeded".lower(),
        "quota exceeded",
        # Duplicate job ID (409 conflict) — seen with dbt-fusion + xdist.
        "error 409",
        # Internal errors surfaced as 503 / "internal error".
        "internal error encountered",
        "503 service unavailable",
        "http 503",
    ),
    "snowflake": (
        "could not connect to snowflake backend",
        "authentication token has expired",
        "incident id:",
        "service is unavailable",
    ),
    "redshift": (
        "connection timed out",
        "could not connect to the server",
        "ssl syscall error",
    ),
    "databricks": _DATABRICKS_PATTERNS,
    "databricks_catalog": _DATABRICKS_PATTERNS,
    "athena": (
        "throttlingexception",
        "toomanyrequestsexception",
        "service unavailable",
    ),
    "dremio": (
        # Common patterns (remotedisconnected, connection was closed) already
        # cover the most frequent Dremio transient errors.  Add Dremio-specific
        # patterns here as they are identified.
    ),
    "postgres": (
        "could not connect to server",
        "connection timed out",
        "server closed the connection unexpectedly",
        "ssl syscall error",
    ),
    "trino": (
        "service unavailable",
        "server returned http response code: 503",
    ),
    "clickhouse": (
        "connection timed out",
        "broken pipe",
    ),
}

# Pre-computed union of all adapter-specific patterns for the unknown-target
# fallback path.  Built once at import time to avoid repeated iteration.
_ALL_ADAPTER_PATTERNS: Tuple[str, ...] = tuple(
    pattern for patterns in _ADAPTER_PATTERNS.values() for pattern in patterns
)


def is_transient_error(
    target: Optional[str],
    output: Optional[str] = None,
    stderr: Optional[str] = None,
) -> bool:
    """Return ``True`` if *output*/*stderr* contain a known transient error.

    Parameters
    ----------
    target:
        The dbt adapter type (e.g. ``"bigquery"``, ``"snowflake"``) **or**
        the dbt profile target name (e.g. ``"dev"``, ``"prod"``).
        When the value matches a key in ``_ADAPTER_PATTERNS``, only that
        adapter's patterns (plus ``_COMMON``) are used.  When it does
        **not** match any known adapter, **all** adapter patterns are
        checked defensively to avoid missing transient errors.
        When ``None``, all adapter patterns are checked defensively.
    output:
        The captured stdout of the dbt command (may be ``None``).
    stderr:
        The captured stderr of the dbt command (may be ``None``).
    """
    haystack = _build_haystack(output, stderr)
    if not haystack:
        return False

    if isinstance(target, str):
        adapter_patterns = _ADAPTER_PATTERNS.get(target.lower())
        if adapter_patterns is not None:
            # Known adapter — use common + adapter-specific patterns.
            patterns: Sequence[str] = (*_COMMON, *adapter_patterns)
        else:
            # Unknown target key (e.g. profile target name). Check all adapters.
            patterns = (*_COMMON, *_ALL_ADAPTER_PATTERNS)
    else:
        # No target provided; still check all adapters defensively.
        patterns = (*_COMMON, *_ALL_ADAPTER_PATTERNS)

    return any(pattern in haystack for pattern in patterns)


def _build_haystack(output: Optional[str] = None, stderr: Optional[str] = None) -> str:
    """Concatenate and lowercase *output* + *stderr* for matching."""
    parts = []
    if output and isinstance(output, str):
        parts.append(output)
    if stderr and isinstance(stderr, str):
        parts.append(stderr)
    return "\n".join(parts).lower()
