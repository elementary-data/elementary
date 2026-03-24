import os
import sys

import yaml

PACKAGES_FILE = "./elementary/monitor/dbt_project/packages.yml"
HELPER_COMMENTS = """
  #  NOTE - for unreleased CLI versions we often need to update the package version to a commit hash (please leave this
  #  commented, so it will be easy to access)
  # - git: https://github.com/elementary-data/dbt-data-reliability.git
  #   revision: <COMMIT_HASH>
  #  When releasing a new version of the package, if the current version is using a commit hash, update the version to the new version.
  # - package: elementary-data/elementary
  #   version: {version}
"""


def bump_packages_version(version: str) -> None:
    with open(PACKAGES_FILE) as f:
        data = yaml.safe_load(f)

    packages = data.get("packages") or []

    new_packages = []
    elementary_found = False
    for pkg in packages:
        if "git" in pkg and "dbt-data-reliability" in pkg["git"]:
            # Replace git hash reference with proper package reference
            new_packages.append(
                {
                    "package": "elementary-data/elementary",
                    "version": version,
                }
            )
            elementary_found = True
        elif pkg.get("package") == "elementary-data/elementary":
            # Update existing package version
            pkg["version"] = version
            new_packages.append(pkg)
            elementary_found = True
        else:
            new_packages.append(pkg)

    if not elementary_found:
        print(
            "::error::Could not find elementary-data/elementary or "
            "dbt-data-reliability entry in packages.yml"
        )
        sys.exit(1)

    data["packages"] = new_packages
    with open(PACKAGES_FILE, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    # Append the helper comments for developer convenience
    with open(PACKAGES_FILE, "a") as f:
        f.write(HELPER_COMMENTS.format(version=version))

    print(f"Updated packages.yml to version {version}")


if __name__ == "__main__":
    version = os.environ.get("PKG_VERSION", "")
    if not version:
        print("::error::PKG_VERSION environment variable is not set")
        sys.exit(1)
    bump_packages_version(version)
