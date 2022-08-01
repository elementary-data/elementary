import os
from pathlib import Path


def is_docker():
    cgroup_path = Path('/proc/self/cgroup')
    return (
            Path('/.dockerenv').exists()
            or
            cgroup_path.is_file() and any('docker' in line for line in cgroup_path.read_text().splitlines())
    )


def is_airflow():
    return "AIRFLOW_CONFIG" in os.environ or "AIRFLOW_HOME" in os.environ
