"""Unit tests for transient-error retry logic in _inner_run_command_with_retries."""

import subprocess
from unittest import mock

import pytest

from elementary.clients.dbt.command_line_dbt_runner import _TRANSIENT_MAX_RETRIES
from elementary.exceptions.exceptions import DbtCommandError

# Patch tenacity wait to zero so tests don't block on exponential backoff.
_ZERO_WAIT = mock.patch(
    "elementary.clients.dbt.command_line_dbt_runner._TRANSIENT_WAIT_MULTIPLIER", 0
)


def _make_runner(**kwargs):
    """Create a SubprocessDbtRunner with deps/packages stubbed out."""
    defaults = dict(
        project_dir="/tmp/fake_project",
        profiles_dir="/tmp/fake_profiles",
        target=None,
        raise_on_failure=True,
        run_deps_if_needed=False,
    )
    defaults.update(kwargs)
    # Use SubprocessDbtRunner but stub out _run_deps_if_needed so it
    # doesn't touch the filesystem.
    from elementary.clients.dbt.subprocess_dbt_runner import SubprocessDbtRunner

    with mock.patch.object(SubprocessDbtRunner, "_run_deps_if_needed"):
        return SubprocessDbtRunner(**defaults)


@_ZERO_WAIT
class TestTransientRetryDbtCommandError:
    """Test retry behaviour when _inner_run_command raises DbtCommandError
    (raise_on_failure=True path)."""

    @mock.patch(
        "elementary.clients.dbt.command_line_dbt_runner.is_transient_error",
        return_value=True,
    )
    @mock.patch("subprocess.run")
    def test_retries_and_reraises_dbt_command_error(
        self, mock_subprocess_run, mock_is_transient
    ):
        """After exhausting retries on a transient DbtCommandError, the original
        DbtCommandError should be re-raised (preserving raise_on_failure contract)."""
        proc_err = subprocess.CalledProcessError(
            1, "dbt run", output=b"connection reset by peer", stderr=b""
        )
        mock_subprocess_run.side_effect = proc_err

        runner = _make_runner(raise_on_failure=True)

        with pytest.raises(DbtCommandError):
            runner.run()

        # _inner_run_command should have been called _TRANSIENT_MAX_RETRIES times
        assert mock_subprocess_run.call_count == _TRANSIENT_MAX_RETRIES

    @mock.patch(
        "elementary.clients.dbt.command_line_dbt_runner.is_transient_error",
        return_value=True,
    )
    @mock.patch("subprocess.run")
    def test_retry_count_matches_config(self, mock_subprocess_run, mock_is_transient):
        """Verify exactly _TRANSIENT_MAX_RETRIES attempts are made."""
        proc_err = subprocess.CalledProcessError(
            1, "dbt test", output=b"connection reset by peer", stderr=b""
        )
        mock_subprocess_run.side_effect = proc_err

        runner = _make_runner(raise_on_failure=True)

        with pytest.raises(DbtCommandError):
            runner.test()

        assert mock_subprocess_run.call_count == _TRANSIENT_MAX_RETRIES


@_ZERO_WAIT
class TestTransientRetryFailedResult:
    """Test retry behaviour when command returns non-success result
    (raise_on_failure=False path)."""

    @mock.patch(
        "elementary.clients.dbt.command_line_dbt_runner.is_transient_error",
        return_value=True,
    )
    @mock.patch("subprocess.run")
    def test_retries_and_returns_last_result(
        self, mock_subprocess_run, mock_is_transient
    ):
        """After exhausting retries on a transient failed result, the last
        DbtCommandResult should be returned (not an exception)."""
        fake_result = mock.MagicMock()
        fake_result.returncode = 1
        fake_result.stdout = b"service unavailable"
        fake_result.stderr = b""
        mock_subprocess_run.return_value = fake_result

        runner = _make_runner(raise_on_failure=False)
        result = runner.run()

        # Should have retried _TRANSIENT_MAX_RETRIES times
        assert mock_subprocess_run.call_count == _TRANSIENT_MAX_RETRIES
        # Result should indicate failure (not raise)
        assert result is False

    @mock.patch(
        "elementary.clients.dbt.command_line_dbt_runner.is_transient_error",
        return_value=True,
    )
    @mock.patch("subprocess.run")
    def test_retry_succeeds_on_second_attempt(
        self, mock_subprocess_run, mock_is_transient
    ):
        """A transient failure followed by success should return the successful result."""
        fail_result = mock.MagicMock()
        fail_result.returncode = 1
        fail_result.stdout = b"service unavailable"
        fail_result.stderr = b""

        success_result = mock.MagicMock()
        success_result.returncode = 0
        success_result.stdout = b"ok"
        success_result.stderr = b""

        mock_subprocess_run.side_effect = [fail_result, success_result]

        runner = _make_runner(raise_on_failure=False)
        result = runner.run()

        assert mock_subprocess_run.call_count == 2
        assert result is True


@_ZERO_WAIT
class TestNonTransientNotRetried:
    """Test that non-transient failures are NOT retried."""

    @mock.patch(
        "elementary.clients.dbt.command_line_dbt_runner.is_transient_error",
        return_value=False,
    )
    @mock.patch("subprocess.run")
    def test_non_transient_error_not_retried(
        self, mock_subprocess_run, mock_is_transient
    ):
        """A non-transient DbtCommandError should propagate immediately
        without any retries."""
        proc_err = subprocess.CalledProcessError(
            1, "dbt run", output=b"syntax error in model", stderr=b""
        )
        mock_subprocess_run.side_effect = proc_err

        runner = _make_runner(raise_on_failure=True)

        with pytest.raises(DbtCommandError):
            runner.run()

        # Only called once — no retry
        assert mock_subprocess_run.call_count == 1

    @mock.patch(
        "elementary.clients.dbt.command_line_dbt_runner.is_transient_error",
        return_value=False,
    )
    @mock.patch("subprocess.run")
    def test_non_transient_failed_result_not_retried(
        self, mock_subprocess_run, mock_is_transient
    ):
        """A non-transient failed result should be returned immediately
        without any retries."""
        fake_result = mock.MagicMock()
        fake_result.returncode = 1
        fake_result.stdout = b"compilation error"
        fake_result.stderr = b""
        mock_subprocess_run.return_value = fake_result

        runner = _make_runner(raise_on_failure=False)
        result = runner.run()

        # Only called once — no retry
        assert mock_subprocess_run.call_count == 1
        assert result is False

    @mock.patch("subprocess.run")
    def test_successful_command_not_retried(self, mock_subprocess_run):
        """A successful command should return immediately without retries."""
        fake_result = mock.MagicMock()
        fake_result.returncode = 0
        fake_result.stdout = b"ok"
        fake_result.stderr = b""
        mock_subprocess_run.return_value = fake_result

        runner = _make_runner(raise_on_failure=False)
        result = runner.run()

        assert mock_subprocess_run.call_count == 1
        assert result is True
