import os

_MONITOR_DIR = os.path.dirname(os.path.realpath(__file__))

_DBT_PACKAGE_NAME = "elementary"

PATH = os.path.join(_MONITOR_DIR, "dbt_project")

# Compatibility for previous dbt versions
_MODULES_PATH = os.path.join(PATH, "dbt_modules", _DBT_PACKAGE_NAME)
_PACKAGES_PATH = os.path.join(PATH, "dbt_packages", _DBT_PACKAGE_NAME)


def dbt_package_exists() -> bool:
    return os.path.exists(_PACKAGES_PATH) or os.path.exists(_MODULES_PATH)
