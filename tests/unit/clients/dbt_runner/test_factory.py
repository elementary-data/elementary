from typing import Optional
from unittest import mock

import pytest
from packaging import version

from elementary.clients.dbt import factory
from elementary.clients.dbt.dbt2_runner import Dbt2Runner
from elementary.clients.dbt.dbt_installation import get_dbt2_binary_path
from elementary.clients.dbt.factory import (
    RunnerMethod,
    get_dbt_runner_class,
    get_dbt_runner_method,
)
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner


def _mock_installation(
    monkeypatch,
    dbt_core_version: Optional[str] = None,
    dbt2_binary_available: bool = False,
):
    monkeypatch.setattr(
        factory,
        "get_dbt_core_version",
        lambda: version.Version(dbt_core_version) if dbt_core_version else None,
    )
    monkeypatch.setattr(
        factory, "is_dbt2_binary_available", lambda: dbt2_binary_available
    )


@pytest.mark.parametrize(
    "dbt_core_version,dbt2_binary_available,expected_method",
    [
        ("1.8.0", False, RunnerMethod.API),
        ("1.10.5", True, RunnerMethod.API),
        ("1.4.0", False, RunnerMethod.SUBPROCESS),
        ("2.0.0b2", False, RunnerMethod.DBT2),
        (None, True, RunnerMethod.DBT2),
        (None, False, RunnerMethod.SUBPROCESS),
    ],
)
def test_get_dbt_runner_method_auto_detection(
    monkeypatch, dbt_core_version, dbt2_binary_available, expected_method
):
    monkeypatch.delenv("DBT_RUNNER_METHOD", raising=False)
    _mock_installation(monkeypatch, dbt_core_version, dbt2_binary_available)
    assert get_dbt_runner_method() == expected_method


@pytest.mark.parametrize(
    "env_value,expected_method",
    [
        ("subprocess", RunnerMethod.SUBPROCESS),
        ("api", RunnerMethod.API),
        ("dbt2", RunnerMethod.DBT2),
        ("fusion", RunnerMethod.FUSION),
    ],
)
def test_get_dbt_runner_method_env_override(monkeypatch, env_value, expected_method):
    monkeypatch.setenv("DBT_RUNNER_METHOD", env_value)
    assert get_dbt_runner_method() == expected_method


def test_get_dbt_runner_class():
    assert get_dbt_runner_class(RunnerMethod.SUBPROCESS) is SubprocessDbtRunner
    assert get_dbt_runner_class(RunnerMethod.DBT2) is Dbt2Runner
    assert get_dbt_runner_class(RunnerMethod.FUSION) is Dbt2Runner


@mock.patch("elementary.clients.dbt.dbt_installation.shutil.which")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_package_version")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_core_version")
def test_dbt2_runner_uses_path_binary_when_no_dbt_core(
    mock_get_dbt_core_version, mock_get_dbt_package_version, mock_which, monkeypatch
):
    monkeypatch.delenv("DBT_FUSION_PATH", raising=False)
    mock_get_dbt_core_version.return_value = None
    mock_get_dbt_package_version.return_value = None
    mock_which.return_value = "/some/venv/bin/dbt"

    assert get_dbt2_binary_path() == "/some/venv/bin/dbt"


@mock.patch("elementary.clients.dbt.dbt_installation.shutil.which")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_package_version")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_core_version")
def test_dbt2_runner_uses_path_binary_when_dbt2_pip_installed_alongside_dbt_core_1x(
    mock_get_dbt_core_version, mock_get_dbt_package_version, mock_which, monkeypatch
):
    monkeypatch.delenv("DBT_FUSION_PATH", raising=False)
    mock_get_dbt_core_version.return_value = version.Version("1.10.0")
    mock_get_dbt_package_version.return_value = version.Version("2.0.0rc212")
    mock_which.return_value = "/some/venv/bin/dbt"

    assert get_dbt2_binary_path() == "/some/venv/bin/dbt"


@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_package_version")
@mock.patch("elementary.clients.dbt.dbt_installation.get_dbt_core_version")
def test_dbt2_runner_ignores_path_binary_when_dbt_core_1x(
    mock_get_dbt_core_version, mock_get_dbt_package_version, monkeypatch
):
    monkeypatch.delenv("DBT_FUSION_PATH", raising=False)
    mock_get_dbt_core_version.return_value = version.Version("1.10.0")
    mock_get_dbt_package_version.return_value = None

    assert get_dbt2_binary_path().endswith("/.local/bin/dbt")


def test_dbt2_runner_honors_dbt_fusion_path_env_var(monkeypatch):
    monkeypatch.setenv("DBT_FUSION_PATH", "/custom/path/dbt")

    assert get_dbt2_binary_path() == "/custom/path/dbt"
