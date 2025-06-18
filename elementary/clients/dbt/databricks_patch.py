"""Databricks compatibility patch for dbt-databricks 1.10.2."""
import logging
from typing import Any, Union

logger = logging.getLogger(__name__)


def is_unsupported_object(model: Any) -> bool:
    """Check if the object is a Macro or other unsupported type"""
    return hasattr(model, "__class__") and "Macro" in str(model.__class__)


def safe_catalog_name(model: Any) -> str:
    try:
        if is_unsupported_object(model):
            logger.debug(
                "Received unsupported object type for catalog_name, using unity as default"
            )
            return "unity"
        # Handle RelationConfig objects
        if hasattr(model, "config") and model.config and hasattr(model.config, "get"):
            catalog = model.config.get("catalog")
            if catalog:
                return catalog
        # Fallback to unity catalog
        return "unity"
    except Exception as e:
        logger.debug(
            f"Failed to parse catalog name from model: {e}. Using unity as default."
        )
        return "unity"


def safe_file_format(model: Any) -> Union[str, None]:
    try:
        if is_unsupported_object(model):
            return None
        return safe_get(model, "file_format")
    except Exception as e:
        logger.debug(f"Failed to get file_format from model: {e}")
        return None


def safe_location_path(model: Any) -> Union[str, None]:
    try:
        if is_unsupported_object(model):
            return None
        if not hasattr(model, "config") or not model.config:
            return None
        if model.config.get("include_full_name_in_path"):
            return f"{model.database}/{model.schema}/{model.identifier}"
        return model.identifier if hasattr(model, "identifier") else None
    except Exception as e:
        logger.debug(f"Failed to get location_path from model: {e}")
        return None


def safe_location_root(model: Any) -> Union[str, None]:
    try:
        if is_unsupported_object(model):
            return None
        return safe_get(model, "location_root")
    except Exception as e:
        logger.debug(f"Failed to get location_root from model: {e}")
        return None


def safe_table_format(model: Any) -> Union[str, None]:
    try:
        if is_unsupported_object(model):
            return None
        return safe_get(model, "table_format")
    except Exception as e:
        logger.debug(f"Failed to get table_format from model: {e}")
        return None


def safe_get(
    model: Any, setting: str, case_sensitive: Union[bool, None] = False
) -> Union[str, None]:
    try:
        if is_unsupported_object(model):
            return None
        # Check if model has config attribute
        if not hasattr(model, "config") or not model.config:
            return None
        # Check if config has get method
        if not hasattr(model.config, "get"):
            return None
        value = model.config.get(setting)
        if value:
            return value if case_sensitive else value.lower()
        return None
    except Exception as e:
        logger.debug(f"Failed to get {setting} from model config: {e}")
        return None


def apply_databricks_patch() -> bool:
    """Apply monkey patch to fix dbt-databricks 1.10.2 compatibility issues.

    Returns:
        bool: True if patch was successfully applied, False otherwise.
    """
    try:
        from dbt.adapters.databricks import parse_model  # type: ignore

        # Replace problematic functions with safe versions
        parse_model.catalog_name = safe_catalog_name
        parse_model.file_format = safe_file_format
        parse_model.location_path = safe_location_path
        parse_model.location_root = safe_location_root
        parse_model.table_format = safe_table_format
        parse_model._get = safe_get

        logger.debug("Applied dbt-databricks 1.10.2 compatibility patch")
        return True

    except ImportError:
        # parse_model module doesn't exist in older versions
        return False
    except Exception as e:
        logger.debug(f"Failed to apply dbt-databricks compatibility patch: {e}")
        return False
