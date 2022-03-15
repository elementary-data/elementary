import subprocess
from typing import Union
import json
from utils.log import get_logger

logger = get_logger(__name__)


class DbtRunner(object):
    ELEMENTARY_LOG_PREFIX = 'Elementary: '
    def __init__(self, project_dir: str, profiles_dir: str) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir

    def _run_command(self, command_args: list, json_logs=False) -> (bool, str):
        dbt_command = ['dbt']
        if json_logs:
            dbt_command.extend(['--log-format', 'json'])
        dbt_command.extend(command_args)
        dbt_command.extend(['--project-dir', self.project_dir])
        dbt_command.extend(['--profiles-dir', self.profiles_dir])
        logger.info(f"Running {' '.join(dbt_command)} (this might take a while)")
        result = subprocess.run(dbt_command, check=False, capture_output=True)
        if result.returncode != 0:
            return False, result.stdout.decode('utf-8')

        return True, result.stdout.decode('utf-8')

    def deps(self) -> bool:
        success, command_output = self._run_command(['deps'])
        logger.info(command_output)
        return success

    def seed(self) -> bool:
        success, command_output = self._run_command(['seed'])
        logger.info(command_output)
        return success

    def snapshot(self) -> bool:
        success, command_output = self._run_command(['snapshot'])
        logger.info(command_output)
        return success

    def run_operation(self, macro_name, json_logs=True) -> Union[None, str]:
        success, command_output = self._run_command(['run-operation', macro_name], json_logs)
        if json_logs:
            json_messages = command_output.splitlines()
            for json_message in json_messages:
                log_message_dict = json.loads(json_message)
                log_message_data_dict = log_message_dict.get('data')
                if log_message_data_dict is not None:
                    log_message = log_message_data_dict.get('msg')
                    if log_message is not None and log_message.startswith(self.ELEMENTARY_LOG_PREFIX):
                        return log_message.replace(self.ELEMENTARY_LOG_PREFIX, '')
        return None

    def run(self, select: Union[str, None] = None, full_refresh: bool = False) -> bool:
        command_args = ['run']
        if full_refresh:
            command_args.append('--full-refresh')
        if select is not None:
            command_args.extend(['-s', select])
        success, command_output = self._run_command(command_args)
        logger.info(command_output)
        return success

    def test(self, select: Union[str, None] = None):
        command_args = ['test']
        if select is not None:
            command_args.extend(['-s', select])
        success, command_output = self._run_command(command_args)
        logger.info(command_output)
        return success
