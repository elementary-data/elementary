"""Helpers for optional dependency imports."""


def import_optional_dependency(module_name: str, extra_name: str):
    """Import and return an optional dependency module, raising a clear error if missing.

    Args:
        module_name: Fully qualified module name (e.g. "boto3", "google.cloud.storage").
        extra_name: The pip extra that provides this dependency (e.g. "s3", "gcs").

    Returns:
        The imported module.

    Raises:
        ImportError: With an actionable message telling the user how to install the extra.
    """
    import importlib

    try:
        return importlib.import_module(module_name)
    except ImportError as exc:
        raise ImportError(
            f"Missing optional dependency '{module_name}'. "
            f"Install it with: pip install 'elementary-data[{extra_name}]'"
        ) from exc
