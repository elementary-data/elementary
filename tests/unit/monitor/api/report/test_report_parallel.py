from unittest.mock import MagicMock, patch

import pytest

from elementary.monitor.api.report.report import ReportAPI


@pytest.fixture
def mock_dbt_runner():
    runner = MagicMock()
    runner.project_dir = "/tmp/project"
    runner.profiles_dir = "/tmp/profiles"
    runner.target = "dev"
    runner.raise_on_failure = True
    runner.env_vars = {"KEY": "value"}
    runner.vars = {}
    runner.secret_vars = {}
    runner.allow_macros_without_package_prefix = False
    return runner


class TestCreateSubprocessRunner:
    def test_creates_runner_with_correct_config(self, mock_dbt_runner):
        api = ReportAPI(mock_dbt_runner)
        with patch(
            "elementary.monitor.api.report.report.SubprocessDbtRunner"
        ) as mock_cls:
            api._create_subprocess_runner()
            mock_cls.assert_called_once_with(
                project_dir="/tmp/project",
                profiles_dir="/tmp/profiles",
                target="dev",
                raise_on_failure=True,
                env_vars={"KEY": "value"},
                vars={},
                secret_vars={},
                allow_macros_without_package_prefix=False,
                run_deps_if_needed=False,
            )

    def test_deps_not_run(self, mock_dbt_runner):
        api = ReportAPI(mock_dbt_runner)
        with patch(
            "elementary.monitor.api.report.report.SubprocessDbtRunner"
        ) as mock_cls:
            api._create_subprocess_runner()
            call_kwargs = mock_cls.call_args[1]
            assert call_kwargs["run_deps_if_needed"] is False


class TestGetReportDataRouting:
    def test_threads_1_uses_sequential(self, mock_dbt_runner):
        api = ReportAPI(mock_dbt_runner)
        with patch.object(api, "_get_report_data_sequential") as mock_seq:
            mock_seq.return_value = (MagicMock(), None)
            api.get_report_data(threads=1)
            mock_seq.assert_called_once()

    def test_threads_gt1_uses_parallel(self, mock_dbt_runner):
        api = ReportAPI(mock_dbt_runner)
        with patch.object(api, "_get_report_data_parallel") as mock_par:
            mock_par.return_value = (MagicMock(), None)
            api.get_report_data(threads=4)
            mock_par.assert_called_once()

    def test_threads_passed_to_parallel(self, mock_dbt_runner):
        api = ReportAPI(mock_dbt_runner)
        with patch.object(api, "_get_report_data_parallel") as mock_par:
            mock_par.return_value = (MagicMock(), None)
            api.get_report_data(threads=8)
            call_kwargs = mock_par.call_args[1]
            assert call_kwargs["threads"] == 8


class TestGetReportDataParallel:
    def test_uses_thread_pool_executor(self, mock_dbt_runner):
        api = ReportAPI(mock_dbt_runner)
        with (
            patch.object(api, "_create_subprocess_runner") as mock_create,
            patch(
                "elementary.monitor.api.report.report.ThreadPoolExecutor"
            ) as mock_pool_cls,
            patch(
                "elementary.monitor.api.report.report.ModelsAPI"
            ),
            patch(
                "elementary.monitor.api.report.report.TestsAPI"
            ),
            patch(
                "elementary.monitor.api.report.report.SourceFreshnessesAPI"
            ),
            patch(
                "elementary.monitor.api.report.report.InvocationsAPI"
            ),
            patch(
                "elementary.monitor.api.report.report.LineageAPI"
            ),
            patch(
                "elementary.monitor.api.report.report.FiltersAPI"
            ),
            patch.object(api, "_assemble_report_data") as mock_assemble,
        ):
            mock_create.return_value = MagicMock()
            mock_pool = MagicMock()
            mock_pool_cls.return_value.__enter__ = MagicMock(return_value=mock_pool)
            mock_pool_cls.return_value.__exit__ = MagicMock(return_value=False)
            mock_pool.submit.return_value.result.return_value = {}
            mock_assemble.return_value = (MagicMock(), None)

            api._get_report_data_parallel(threads=4)

            mock_pool_cls.assert_called_with(max_workers=4)

    def test_error_propagation(self, mock_dbt_runner):
        api = ReportAPI(mock_dbt_runner)
        error = RuntimeError("test error")
        with patch.object(
            api, "_create_subprocess_runner", side_effect=error
        ):
            result, err = api._get_report_data_parallel(threads=4)
            assert err is error
