import os
import sys

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

PACKAGES_FILE = "./elementary/monitor/dbt_project/packages.yml"


def bump_packages_version(version: str) -> None:
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)

    with open(PACKAGES_FILE) as f:
        data = yaml.load(f)

    packages = data.get("packages") or []
    elementary_found = False
    for i, pkg in enumerate(packages):
        if "git" in pkg and "dbt-data-reliability" in pkg["git"]:
            new_pkg = CommentedMap()
            new_pkg["package"] = "elementary-data/elementary"
            new_pkg["version"] = version
            packages[i] = new_pkg
            elementary_found = True
            break
        if pkg.get("package") == "elementary-data/elementary":
            pkg["version"] = version
            elementary_found = True
            break

    if not elementary_found:
        print(
            "::error::Could not find elementary-data/elementary or "
            "dbt-data-reliability entry in packages.yml"
        )
        sys.exit(1)

    with open(PACKAGES_FILE, "w") as f:
        yaml.dump(data, f)

    print(f"Updated packages.yml to version {version}")


if __name__ == "__main__":
    version = os.environ.get("PKG_VERSION", "")
    if not version:
        print("::error::PKG_VERSION environment variable is not set")
        sys.exit(1)
    bump_packages_version(version)
