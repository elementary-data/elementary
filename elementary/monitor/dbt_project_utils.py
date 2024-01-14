import os
from typing import Optional

from elementary.utils.ordered_yaml import OrderedYaml

_MONITOR_DIR = os.path.dirname(os.path.realpath(__file__))

_DBT_PACKAGE_NAME = "elementary"
_DBT_PROJECT_FILENAME = "dbt_project.yml"
_PACKAGES_FILENAME = "packages.yml"

CLI_DBT_PROJECT_PATH = os.path.join(_MONITOR_DIR, "dbt_project")


def is_dbt_package_up_to_date(project_dir: str) -> bool:
    installed_version = _get_installed_dbt_package_version(project_dir)
    if installed_version is None:
        return False

    required_version = _get_required_dbt_package_version(project_dir)
    # We're using a non dbt Hub requirement, such as Git or local.
    if not required_version:
        return True

    return installed_version == required_version


def _get_elementary_package_path(project_dir: str) -> Optional[str]:
    package_path = os.path.join(project_dir, "dbt_packages", _DBT_PACKAGE_NAME)
    if os.path.exists(package_path):
        return package_path

    return None


def _get_installed_dbt_package_version(project_dir: str) -> Optional[str]:
    package_path = _get_elementary_package_path(project_dir)
    if package_path is None:
        return None

    project_path = os.path.join(package_path, _DBT_PROJECT_FILENAME)
    if not os.path.exists(package_path):
        return None

    project_yaml_dict = OrderedYaml().load(project_path)
    return project_yaml_dict["version"]


def _get_required_dbt_package_version(project_dir: str) -> Optional[str]:
    packages_file_path = os.path.join(project_dir, _PACKAGES_FILENAME)
    packages_yaml = OrderedYaml().load(packages_file_path)

    for requirement in packages_yaml.get("packages", []):
        package_id = requirement.get("package")
        if not package_id:
            continue
        package_name = package_id.split("/")[-1]
        if package_name == _DBT_PACKAGE_NAME:
            return requirement["version"]
    return None
