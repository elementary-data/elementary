import shutil
from pathlib import Path
from tempfile import mkdtemp

import env
import pytest
from dbt.version import __version__ as dbt_version
from packaging import version

from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner

DBT_PROJECT_PATH = Path(__file__).parent / "dbt_project"


def pytest_addoption(parser):
    parser.addoption("--target", action="store", default="postgres")
    parser.addoption("--skip-init", action="store_true", default=False)


@pytest.fixture(scope="session")
def project_dir_copy():
    dbt_project_copy_dir = mkdtemp(prefix="integration_tests_project_")
    try:
        shutil.copytree(
            DBT_PROJECT_PATH,
            dbt_project_copy_dir,
            dirs_exist_ok=True,
            symlinks=True,
        )
        yield dbt_project_copy_dir
    finally:
        shutil.rmtree(dbt_project_copy_dir)


@pytest.fixture(scope="session", autouse=True)
def init_tests_env(dbt_runner, skip_init):
    if not skip_init:
        env.init(dbt_runner)


@pytest.fixture(autouse=True)
def skip_by_targets(request, target: str):
    if request.node.get_closest_marker("skip_targets"):
        skipped_targets = request.node.get_closest_marker("skip_targets").args[0]
        if target in skipped_targets:
            pytest.skip("Test unsupported for target: {}".format(target))


@pytest.fixture(autouse=True)
def only_on_targets(request, target: str):
    if request.node.get_closest_marker("only_on_targets"):
        requested_targets = request.node.get_closest_marker("only_on_targets").args[0]
        if target not in requested_targets:
            pytest.skip("Test unsupported for target: {}".format(target))


@pytest.fixture(autouse=True)
def requires_dbt_version(request):
    if request.node.get_closest_marker("requires_dbt_version"):
        required_version = request.node.get_closest_marker("requires_dbt_version").args[
            0
        ]
        if version.parse(dbt_version) < version.parse(required_version):
            pytest.skip(
                "Test requires dbt version {} or above, but {} is installed.".format(
                    required_version, dbt_version
                )
            )


@pytest.fixture(scope="session")
def target(request) -> str:
    return request.config.getoption("--target")


@pytest.fixture(scope="session")
def dbt_runner(target: str, project_dir_copy: str) -> SubprocessDbtRunner:
    return SubprocessDbtRunner(
        project_dir_copy,
        target=target,
        vars={
            "debug_logs": True,
        },
        raise_on_failure=False,
    )


@pytest.fixture(scope="session")
def skip_init(request) -> str:
    return request.config.getoption("--skip-init")


@pytest.fixture
def test_id(request) -> str:
    if request.cls:
        return f"{request.cls.__name__}_{request.node.name}"
    return request.node.name
