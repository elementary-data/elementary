"""Per-adapter transient error patterns for automatic retry.

Each adapter may produce transient errors that are safe to retry.  This
module centralises those patterns so that the runner can decide whether a
failed dbt command should be retried transparently.

To add patterns for a new adapter, append a new entry to
``_ADAPTER_PATTERNS`` with the *target name* as key and a tuple of
**plain, lowercase** substrings that appear in the error output.
Matching is case-insensitive substring search so regex is not needed.
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
    "connection refused",
    "read timed out",
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
        # Internal errors surfaced as 503 / "internal error".
        "internal error encountered",
        "503",
    ),
    "snowflake": (
        "could not connect to snowflake backend",
        "authentication token has expired",
        "incident",
        "service is unavailable",
    ),
    "redshift": (
        "connection timed out",
        "could not connect to the server",
        "ssl syscall error",
    ),
    "databricks": (
        "temporarily_unavailable",
        "504 gateway timeout",
        "502 bad gateway",
        "service unavailable",
    ),
    "databricks_catalog": (
        "temporarily_unavailable",
        "504 gateway timeout",
        "502 bad gateway",
        "service unavailable",
    ),
    "athena": (
        "throttlingexception",
        "toomanyrequestsexception",
        "service unavailable",
    ),
    "dremio": (
        "remotedisconnected",
        "connection was closed",
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


def is_transient_error(
    target: Optional[str],
    output: Optional[str] = None,
    stderr: Optional[str] = None,
) -> bool:
    """Return ``True`` if *output*/*stderr* contain a known transient error.

    Parameters
    ----------
    target:
        The dbt target name (e.g. ``"bigquery"``, ``"snowflake"``).
        When ``None`` only the common patterns are checked.
    output:
        The captured stdout of the dbt command (may be ``None``).
    stderr:
        The captured stderr of the dbt command (may be ``None``).
    """
    haystack = _build_haystack(output, stderr)
    if not haystack:
        return False

    patterns: Sequence[str] = _COMMON
    if target is not None:
        adapter_patterns = _ADAPTER_PATTERNS.get(target.lower(), ())
        patterns = (*_COMMON, *adapter_patterns)

    return any(pattern in haystack for pattern in patterns)


def _build_haystack(output: Optional[str] = None, stderr: Optional[str] = None) -> str:
    """Concatenate and lowercase *output* + *stderr* for matching."""
    parts = []
    if output:
        parts.append(output)
    if stderr:
        parts.append(stderr)
    return "\n".join(parts).lower()
