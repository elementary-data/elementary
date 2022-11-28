import os
import shutil

_MONITOR_DIR = os.path.dirname(os.path.realpath(__file__))

_DBT_PACKAGE_NAME = "elementary"

PATH = os.path.join(_MONITOR_DIR, "dbt_project")

# Compatibility for previous dbt versions
_MODULES_PATH = os.path.join(PATH, "dbt_modules")
_PACKAGES_PATH = os.path.join(PATH, "dbt_packages")


def dbt_packages_exist() -> bool:
    return os.path.exists(_PACKAGES_PATH) or os.path.exists(_MODULES_PATH)


def clear_dbt_packages():
    if os.path.exists(_PACKAGES_PATH):
        shutil.rmtree(_PACKAGES_PATH)
    if os.path.exists(_MODULES_PATH):
        shutil.rmtree(_MODULES_PATH)
