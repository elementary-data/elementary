import os
import platform
from pathlib import Path

import elementary.utils.package


def _is_docker():
    cgroup_path = Path("/proc/self/cgroup")
    return (
        Path("/.dockerenv").exists()
        or cgroup_path.is_file()
        and any("docker" in line for line in cgroup_path.read_text().splitlines())
    )


def _is_airflow():
    return "AIRFLOW_CONFIG" in os.environ or "AIRFLOW_HOME" in os.environ


def _is_github_actions():
    return "GITHUB_ACTIONS" in os.environ


def _is_elementary_hosted():
    return "ELEMENTARY_HOSTED" in os.environ


def get_props():
    return {
        "os": platform.system(),
        "is_docker": _is_docker(),
        "is_airflow": _is_airflow(),
        "is_github_actions": _is_github_actions(),
        "is_elementary_hosted": _is_elementary_hosted(),
        "python_version": platform.python_version(),
        "elementary_version": elementary.utils.package.get_package_version(),
    }
