import shutil
import sys
from pathlib import Path

from elementary.monitor.dbt_project_utils import _DBT_PACKAGE_NAME, _PACKAGES_PATH

ELE_DBT_PKG_PATH = Path(_PACKAGES_PATH) / _DBT_PACKAGE_NAME


def main():
    if len(sys.argv) != 2:
        raise ValueError(
            "Please provide the path to the local elementary dbt package as an argument."
        )
    local_dbt_pkg_path = Path(sys.argv[1]).resolve()
    if ELE_DBT_PKG_PATH.is_symlink():
        ELE_DBT_PKG_PATH.unlink()
    if ELE_DBT_PKG_PATH.is_dir():
        shutil.rmtree(ELE_DBT_PKG_PATH)
    ELE_DBT_PKG_PATH.symlink_to(local_dbt_pkg_path)


if __name__ == "__main__":
    main()
