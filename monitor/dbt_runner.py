import subprocess
from typing import Union
import json
from utils.log import get_logger

logger = get_logger(__name__)


class DbtRunner(object):
    ELEMENTARY_LOG_PREFIX = 'Elementary: '

    def __init__(self, project_dir: str, profiles_dir: str, target: Union[str, None] = None) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target

    def _run_command(self, command_args: list, json_logs=False) -> (bool, str):
        dbt_command = ['dbt']
        capture_output = False
        if json_logs:
            dbt_command.extend(['--log-format', 'json'])
            capture_output = True
        dbt_command.extend(command_args)
        dbt_command.extend(['--project-dir', self.project_dir])
        dbt_command.extend(['--profiles-dir', self.profiles_dir])
        if self.target:
            dbt_command.extend(['--target', self.target])
        logger.info(f"Running {' '.join(dbt_command)} (this might take a while)")
        result = subprocess.run(dbt_command, check=False, capture_output=capture_output)
        output = None
        if capture_output:
            output = result.stdout.decode('utf-8')
        if result.returncode != 0:
            return False, output

        return True, output

    def deps(self) -> bool:
        success, _ = self._run_command(['deps'])
        return success

    def seed(self, select: Union[str, None] = None) -> bool:
        command_args = ['seed']
        if select is not None:
            command_args.extend(['-s', select])
        success, _ = self._run_command(command_args)
        return success

    def snapshot(self) -> bool:
        success, _ = self._run_command(['snapshot'])
        return success

    def run_operation(self, macro_name: str, json_logs: bool = True, macro_args: dict = None) -> list:
        command_args = ['run-operation', macro_name]
        if macro_args is not None:
            json_args = json.dumps(macro_args)
            command_args.extend(['--args', json_args])
        success, command_output = self._run_command(command_args, json_logs)
        run_operation_results = []
        if json_logs:
            json_messages = command_output.splitlines()
            for json_message in json_messages:
                log_message_dict = json.loads(json_message)
                log_message_data_dict = log_message_dict.get('data')
                if log_message_data_dict is not None:
                    log_message = log_message_data_dict.get('msg')
                    if log_message is not None and log_message.startswith(self.ELEMENTARY_LOG_PREFIX):
                        run_operation_results.append(log_message.replace(self.ELEMENTARY_LOG_PREFIX, ''))
        return run_operation_results

    def run(self, models: Union[str, None] = None, select: Union[str, None] = None, full_refresh: bool = False) -> bool:
        command_args = ['run']
        if full_refresh:
            command_args.append('--full-refresh')
        if models is not None:
            command_args.extend(['-m', models])
        if select is not None:
            command_args.extend(['-s', select])
        success, _ = self._run_command(command_args)
        return success

    def test(self, select: Union[str, None] = None):
        command_args = ['test']
        if select is not None:
            command_args.extend(['-s', select])
        success, _ = self._run_command(command_args)
        return success
