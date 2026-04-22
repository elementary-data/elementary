from contextlib import contextmanager
from unittest import mock

from dbt.cli.main import dbtRunnerResult

from elementary.clients.dbt.api_dbt_runner import APIDbtRunner


def _make_result(success=True, exception=None):
    return dbtRunnerResult(
        success=success,
        result=None,
        exception=exception,
    )


def _make_runner():
    runner = APIDbtRunner.__new__(APIDbtRunner)
    runner._manifest = None
    runner.project_dir = "/tmp/fake"
    runner.env_vars = None
    runner.raise_on_failure = False
    return runner


@contextmanager
def _noop_context(*args, **kwargs):
    yield


_PATCH_CHDIR = mock.patch("elementary.clients.dbt.api_dbt_runner.with_chdir", _noop_context)
_PATCH_ENV = mock.patch("elementary.clients.dbt.api_dbt_runner.env_vars_context", _noop_context)


@_PATCH_ENV
@_PATCH_CHDIR
@mock.patch("elementary.clients.dbt.api_dbt_runner.dbtRunner")
def test_manifest_cached_after_first_success(mock_dbt_runner_cls):
    fake_manifest = object()
    mock_instance = mock.MagicMock()
    mock_instance.invoke.return_value = _make_result(success=True)
    mock_instance.manifest = fake_manifest
    mock_dbt_runner_cls.return_value = mock_instance

    runner = _make_runner()
    runner._inner_run_command(["run-operation", "foo"], quiet=True, log_output=False, log_format="json")

    assert runner._manifest is fake_manifest
    mock_dbt_runner_cls.assert_called_once_with(manifest=None, callbacks=mock.ANY)


@_PATCH_ENV
@_PATCH_CHDIR
@mock.patch("elementary.clients.dbt.api_dbt_runner.dbtRunner")
def test_manifest_not_cached_on_failure(mock_dbt_runner_cls):
    mock_instance = mock.MagicMock()
    mock_instance.invoke.return_value = _make_result(success=False)
    mock_instance.manifest = object()
    mock_dbt_runner_cls.return_value = mock_instance

    runner = _make_runner()
    runner._inner_run_command(["run-operation", "foo"], quiet=True, log_output=False, log_format="json")

    assert runner._manifest is None


@_PATCH_ENV
@_PATCH_CHDIR
@mock.patch("elementary.clients.dbt.api_dbt_runner.dbtRunner")
def test_cached_manifest_reused_on_subsequent_calls(mock_dbt_runner_cls):
    fake_manifest = object()
    mock_instance = mock.MagicMock()
    mock_instance.invoke.return_value = _make_result(success=True)
    mock_instance.manifest = fake_manifest
    mock_dbt_runner_cls.return_value = mock_instance

    runner = _make_runner()

    runner._inner_run_command(["run-operation", "foo"], quiet=True, log_output=False, log_format="json")
    assert runner._manifest is fake_manifest

    new_manifest = object()
    mock_instance.manifest = new_manifest
    mock_dbt_runner_cls.reset_mock()

    runner._inner_run_command(["run-operation", "bar"], quiet=True, log_output=False, log_format="json")

    mock_dbt_runner_cls.assert_called_once_with(manifest=fake_manifest, callbacks=mock.ANY)
    assert runner._manifest is fake_manifest
