import json
from unittest import mock

import pytest

from elementary.clients.dbt.factory import create_internal_dbt_runner
from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner
from elementary.config.config import Config
from elementary.monitor.data_monitoring.data_monitoring import DataMonitoring
from elementary.monitor.dbt_project_utils import CLI_DBT_PROJECT_PATH
from elementary.monitor.debug import Debug
from elementary.operations.upload_source_freshness import UploadSourceFreshnessOperation
from tests.mocks.data_monitoring.alerts.data_monitoring_alerts_mock import (
    DataMonitoringAlertsMock,
)

DBT_VARS = {"query_max_size": 1000}


@pytest.fixture
def config(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path))
    return Config(
        config_dir=str(tmp_path),
        target_path=str(tmp_path),
        profiles_dir="profiles_dir",
        profile_target="target",
        dbt_quoting="all",
        run_dbt_deps_if_needed=False,
        dbt_vars=DBT_VARS,
    )


@pytest.fixture
def mock_create_dbt_runner():
    with mock.patch(
        "elementary.clients.dbt.factory.create_dbt_runner"
    ) as create_dbt_runner:
        yield create_dbt_runner


def _assert_internal_runner_created(
    mock_create_dbt_runner, config: Config, force_dbt_deps: bool = False
):
    mock_create_dbt_runner.assert_called_once_with(
        CLI_DBT_PROJECT_PATH,
        config.profiles_dir,
        config.profile_target,
        env_vars=config.env_vars,
        vars=DBT_VARS,
        run_deps_if_needed=False,
        force_dbt_deps=force_dbt_deps,
    )


def test_config_dbt_vars_default(tmp_path, monkeypatch):
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path))
    assert Config(config_dir=str(tmp_path), target_path=str(tmp_path)).dbt_vars is None


@pytest.mark.parametrize("force_dbt_deps", [True, False])
def test_create_internal_dbt_runner(config, mock_create_dbt_runner, force_dbt_deps):
    runner = create_internal_dbt_runner(config, force_dbt_deps=force_dbt_deps)
    assert runner is mock_create_dbt_runner.return_value
    _assert_internal_runner_created(
        mock_create_dbt_runner, config, force_dbt_deps=force_dbt_deps
    )


def test_data_monitoring_internal_runner_vars(config, mock_create_dbt_runner):
    data_monitoring = DataMonitoring.__new__(DataMonitoring)
    data_monitoring.config = config
    data_monitoring.force_update_dbt_package = True
    data_monitoring._init_internal_dbt_runner()
    _assert_internal_runner_created(mock_create_dbt_runner, config, force_dbt_deps=True)


def test_debug_internal_runner_vars(config, mock_create_dbt_runner):
    assert Debug(config).run()
    _assert_internal_runner_created(mock_create_dbt_runner, config)


def test_upload_source_freshness_internal_runner_vars(config, mock_create_dbt_runner):
    UploadSourceFreshnessOperation(config).upload_results(
        results=[], metadata={"invocation_id": "id"}, rows_per_insert=10
    )
    _assert_internal_runner_created(mock_create_dbt_runner, config)


@mock.patch("subprocess.run")
def test_runner_vars_merged_with_per_call_vars(mock_subprocess_run):
    runner = SubprocessDbtRunner(
        project_dir="proj_dir",
        profiles_dir="prof_dir",
        vars={"query_max_size": 1000, "days_back": 30},
        run_deps_if_needed=False,
    )
    runner.run(select="model", vars={"days_back": 7})
    args = mock_subprocess_run.call_args[0][0]
    assert json.loads(args[args.index("--vars") + 1]) == {
        "query_max_size": 1000,
        "days_back": 7,
    }


def test_populate_alerts_uses_runner_vars():
    data_monitoring_alerts = DataMonitoringAlertsMock()
    with mock.patch.object(
        data_monitoring_alerts.internal_dbt_runner, "run", return_value=True
    ) as run:
        assert data_monitoring_alerts._populate_data()
    run.assert_called_once_with(
        select="elementary_cli.alerts.alerts_v2",
        full_refresh=False,
    )
