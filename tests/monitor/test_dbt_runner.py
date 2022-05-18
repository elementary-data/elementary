import pytest
from unittest import mock
from monitor.dbt_runner import DbtRunner


@pytest.mark.parametrize("command", [
    'seed',
    'snapshot',
    'deps',
])
@mock.patch('subprocess.run')
def test_dbt_runner_seed(mock_subprocess_run, command):
    project_dir = 'proj_dir'
    profiles_dir = 'prof_dir'
    dbt_runner = DbtRunner(project_dir=project_dir, profiles_dir=profiles_dir)
    if command == 'seed':
        dbt_runner.seed()
    elif command == 'snapshot':
        dbt_runner.snapshot()
    elif command == 'deps':
        dbt_runner.deps()
    mock_subprocess_run.assert_called()
    mock_subprocess_run.asset_has_calls(mock.call(['dbt',
                                                   command,
                                                   '--project-dir',
                                                   'proj_dir',
                                                   '--profiles-dir',
                                                   'prof_dir'], mock.ANY, mock.ANY))


@pytest.mark.parametrize("model,full_refresh", [
    ('m1', True),
    ('m1', False),
    (None, True),
    (None, False),
])
@mock.patch('subprocess.run')
def test_dbt_runner_run(mock_subprocess_run, model, full_refresh):
    project_dir = 'proj_dir'
    profiles_dir = 'prof_dir'
    dbt_runner = DbtRunner(project_dir=project_dir, profiles_dir=profiles_dir)
    dbt_runner.run(model, full_refresh=full_refresh)
    mock_subprocess_run.assert_called()
    if model is not None:
        assert model in mock_subprocess_run.call_args[0][0]
    if full_refresh:
        assert '--full-refresh' in mock_subprocess_run.call_args[0][0]

