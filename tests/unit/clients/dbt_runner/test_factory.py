from typing import Optional
from unittest import mock

import pytest
from packaging import version

from elementary.clients.dbt import factory
from elementary.clients.dbt.dbt2_runner import Dbt2Runner
from elementary.clients.dbt.dbt_installation import (
    get_dbt2_binary_path,
    get_dbt2_package_version,
    is_dbt2_binary_available,
)
from elementary.clients.dbt.factory import (
    RunnerMethod,
    get_dbt_runner_class,
    get_dbt_runner_method,
)
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner
from elementary.exceptions.exceptions import NoDbtInstallationError


def _mock_installation(
    monkeypatch,
    dbt_core_version: Optional[str] = None,
    dbt2_binary_available: bool = False,
    dbt_on_path: bool = False,
):
    monkeypatch.setattr(
        factory,
        "get_dbt_core_version",
        lambda: version.Version(dbt_core_version) if dbt_core_version else None,
    )
    monkeypatch.setattr(
        factory, "is_dbt2_binary_available", lambda: dbt2_binary_available
    )
    monkeypatch.setattr(
        factory.shutil, "which", lambda name: "/usr/bin/dbt" if dbt_on_path else None
    )


@pytest.mark.parametrize(
    "dbt_core_version,dbt2_binary_available,dbt_on_path,expected_method",
    [
        ("1.8.0", False, True, RunnerMethod.DBT1_API),
        # dbt v2 takes precedence when installed alongside dbt-core 1.x
        ("1.10.5", True, True, RunnerMethod.DBT2),
        ("1.4.0", False, True, RunnerMethod.DBT1_SUBPROCESS),
        ("2.0.0b2", False, True, RunnerMethod.DBT2),
        (None, True, True, RunnerMethod.DBT2),
        # dbt installed without pip metadata (e.g. system-wide dbt 1.x)
        (None, False, True, RunnerMethod.DBT1_SUBPROCESS),
    ],
)
def test_get_dbt_runner_method_auto_detection(
    monkeypatch, dbt_core_version, dbt2_binary_available, dbt_on_path, expected_method
):
    monkeypatch.delenv("DBT_RUNNER_METHOD", raising=False)
    _mock_installation(
        monkeypatch, dbt_core_version, dbt2_binary_available, dbt_on_path
    )
    assert get_dbt_runner_method() == expected_method


def test_get_dbt_runner_method_raises_when_no_dbt_installation(monkeypatch):
    monkeypatch.delenv("DBT_RUNNER_METHOD", raising=False)
    _mock_installation(monkeypatch, dbt_core_version=None, dbt2_binary_available=False)
    with pytest.raises(NoDbtInstallationError):
        get_dbt_runner_method()


def test_get_dbt_runner_method_hints_when_dbt_core_and_dbt2_coexist(
    monkeypatch, caplog
):
    monkeypatch.delenv("DBT_RUNNER_METHOD", raising=False)
    _mock_installation(
        monkeypatch, dbt_core_version="1.10.0", dbt2_binary_available=True
    )
    with caplog.at_level("INFO"):
        assert get_dbt_runner_method() == RunnerMethod.DBT2
    assert any("using dbt v2" in record.message for record in caplog.records)


@pytest.mark.parametrize(
    "env_value,expected_method",
    [
        ("subprocess", RunnerMethod.DBT1_SUBPROCESS),
        ("api", RunnerMethod.DBT1_API),
        ("dbt2", RunnerMethod.DBT2),
        ("fusion", RunnerMethod.FUSION),
    ],
)
def test_get_dbt_runner_method_env_override(monkeypatch, env_value, expected_method):
    monkeypatch.setenv("DBT_RUNNER_METHOD", env_value)
    assert get_dbt_runner_method() == expected_method


def test_get_dbt_runner_class():
    assert get_dbt_runner_class(RunnerMethod.DBT1_SUBPROCESS) is SubprocessDbtRunner
    assert get_dbt_runner_class(RunnerMethod.DBT2) is Dbt2Runner
    assert get_dbt_runner_class(RunnerMethod.FUSION) is Dbt2Runner


def test_runner_method_legacy_aliases():
    assert RunnerMethod.API is RunnerMethod.DBT1_API
    assert RunnerMethod.SUBPROCESS is RunnerMethod.DBT1_SUBPROCESS


@mock.patch("elementary.clients.dbt.dbt_installation.shutil.which")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt2_package_version")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_core_version")
def test_dbt2_runner_uses_path_binary_when_no_dbt_core(
    mock_get_dbt_core_version, mock_get_dbt2_package_version, mock_which, monkeypatch
):
    monkeypatch.delenv("DBT_FUSION_PATH", raising=False)
    mock_get_dbt_core_version.return_value = None
    mock_get_dbt2_package_version.return_value = None
    mock_which.return_value = "/some/venv/bin/dbt"

    assert get_dbt2_binary_path() == "/some/venv/bin/dbt"


@mock.patch("elementary.clients.dbt.dbt_installation.shutil.which")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt2_package_version")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_core_version")
def test_dbt2_runner_uses_path_binary_when_dbt2_pip_installed_alongside_dbt_core_1x(
    mock_get_dbt_core_version, mock_get_dbt2_package_version, mock_which, monkeypatch
):
    monkeypatch.delenv("DBT_FUSION_PATH", raising=False)
    mock_get_dbt_core_version.return_value = version.Version("1.10.0")
    mock_get_dbt2_package_version.return_value = version.Version("2.0.6")
    mock_which.return_value = "/some/venv/bin/dbt"

    assert get_dbt2_binary_path() == "/some/venv/bin/dbt"


@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt2_package_version")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_core_version")
def test_dbt2_runner_ignores_path_binary_when_dbt_core_1x(
    mock_get_dbt_core_version, mock_get_dbt2_package_version, monkeypatch
):
    monkeypatch.delenv("DBT_FUSION_PATH", raising=False)
    mock_get_dbt_core_version.return_value = version.Version("1.10.0")
    mock_get_dbt2_package_version.return_value = None

    assert get_dbt2_binary_path().endswith("/.local/bin/dbt")


def test_dbt2_runner_honors_dbt_fusion_path_env_var(monkeypatch):
    monkeypatch.setenv("DBT_FUSION_PATH", "/custom/path/dbt")

    assert get_dbt2_binary_path() == "/custom/path/dbt"


@mock.patch("elementary.clients.dbt.dbt_installation.os.path.exists")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt2_package_version")
def test_dbt2_binary_available_when_dbt_fusion_path_env_var_points_to_binary(
    mock_get_dbt2_package_version, mock_exists, monkeypatch
):
    monkeypatch.setenv("DBT_FUSION_PATH", "/custom/path/dbt")
    mock_get_dbt2_package_version.return_value = None
    mock_exists.side_effect = lambda path: path == "/custom/path/dbt"

    assert is_dbt2_binary_available()


@mock.patch("elementary.clients.dbt.dbt_installation.os.path.exists")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt2_package_version")
def test_dbt2_binary_not_available_when_nothing_installed(
    mock_get_dbt2_package_version, mock_exists, monkeypatch
):
    monkeypatch.delenv("DBT_FUSION_PATH", raising=False)
    mock_get_dbt2_package_version.return_value = None
    mock_exists.return_value = False

    assert not is_dbt2_binary_available()


@pytest.mark.parametrize(
    "installed_packages,expected_version",
    [
        ({"dbt": "2.0.6"}, "2.0.6"),
        ({"dbt-oss": "2.0.5"}, "2.0.5"),
        # A 1.x `dbt` package is not a dbt v2 installation
        ({"dbt": "1.8.0"}, None),
        ({}, None),
    ],
)
@mock.patch("elementary.clients.dbt.dbt_installation._get_package_version")
def test_get_dbt2_package_version(
    mock_get_package_version, installed_packages, expected_version
):
    mock_get_package_version.side_effect = lambda name: (
        version.Version(installed_packages[name])
        if name in installed_packages
        else None
    )

    expected = version.Version(expected_version) if expected_version else None
    assert get_dbt2_package_version() == expected
