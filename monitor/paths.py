import os

_MONITOR_DIR = os.path.dirname(os.path.realpath(__file__))

_DBT_PACKAGE_NAME = 'elementary'

DBT_PROJECT_PATH = os.path.join(_MONITOR_DIR, 'dbt_project')

# Compatibility for previous dbt versions
DBT_PROJECT_MODULES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_modules', _DBT_PACKAGE_NAME)
DBT_PROJECT_PACKAGES_PATH = os.path.join(DBT_PROJECT_PATH, 'dbt_packages', _DBT_PACKAGE_NAME)
