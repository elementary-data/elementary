import json
import subprocess
from typing import List, Optional, Tuple

from elementary.utils.log import get_logger

logger = get_logger(__name__)


class DbtRunner:
    ELEMENTARY_LOG_PREFIX = 'Elementary: '

    def __init__(self, project_dir: str, profiles_dir: str, target: Optional[str] = None) -> None:
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target

    def _run_command(
            self,
            command_args: List[str],
            json_logs: bool = False,
            vars: Optional[dict] = None,
            quiet: bool = False
    ) -> Tuple[bool, str]:
        dbt_command = ['dbt']
        json_output = False
        if json_logs:
            dbt_command.extend(['--log-format', 'json'])
            json_output = True
        dbt_command.extend(command_args)
        dbt_command.extend(['--project-dir', self.project_dir])
        dbt_command.extend(['--profiles-dir', self.profiles_dir])
        if self.target:
            dbt_command.extend(['--target', self.target])
        if vars:
            json_vars = json.dumps(vars)
            dbt_command.extend(['--vars', json_vars])
        if not quiet:
            logger.info(f"Running {' '.join(dbt_command)} (this might take a while)")
        result = subprocess.run(dbt_command, check=False, capture_output=(json_output or quiet))
        output = None
        if json_output:
            output = result.stdout.decode('utf-8')
            logger.debug(f'Output: {output}')
        if result.returncode != 0:
            return False, output

        return True, output

    def deps(self, quiet: bool = False) -> bool:
        success, _ = self._run_command(command_args=['deps'], quiet=quiet)
        return success

    def seed(self, select: Optional[str] = None) -> bool:
        command_args = ['seed']
        if select:
            command_args.extend(['-s', select])
        success, _ = self._run_command(command_args)
        return success

    def snapshot(self) -> bool:
        success, _ = self._run_command(['snapshot'])
        return success

    def run_operation(
            self,
            macro_name: str,
            json_logs: bool = True,
            macro_args: Optional[dict] = None,
            log_errors: bool = False,
            vars: Optional[dict] = None,
            quiet: bool = False
    ) -> list:
        command_args = ['run-operation', macro_name]
        if macro_args:
            json_args = json.dumps(macro_args)
            command_args.extend(['--args', json_args])
        success, command_output = self._run_command(command_args=command_args, json_logs=json_logs, vars=vars, quiet=quiet)
        if log_errors and not success:
            logger.error(f'Failed to run macro: "{macro_name}"')
        run_operation_results = []
        if json_logs:
            json_messages = command_output.splitlines()
            for json_message in json_messages:
                log_message_dict = json.loads(json_message)
                log_message_data_dict = log_message_dict.get('data')
                if log_message_data_dict is not None:
                    if log_errors and log_message_dict['level'] == 'error':
                        logger.error(log_message_data_dict)
                        continue
                    log_message = log_message_data_dict.get('msg')
                    if log_message is not None and log_message.startswith(self.ELEMENTARY_LOG_PREFIX):
                        run_operation_results.append(log_message.replace(self.ELEMENTARY_LOG_PREFIX, ''))
        return run_operation_results

    def run(
            self,
            models: Optional[str] = None,
            select: Optional[str] = None,
            full_refresh: bool = False,
            vars: Optional[dict] = None,
            quiet: bool = False
    ) -> bool:
        command_args = ['run']
        if full_refresh:
            command_args.append('--full-refresh')
        if models:
            command_args.extend(['-m', models])
        if select:
            command_args.extend(['-s', select])
        success, _ = self._run_command(command_args=command_args, vars=vars, quiet=quiet)
        return success

    def test(
            self,
            select: Optional[str] = None,
            vars: Optional[dict] = None,
            quiet: bool = False
    ) -> bool:
        command_args = ['test']
        if select:
            command_args.extend(['-s', select])
        success, _ = self._run_command(command_args=command_args, vars=vars, quiet=quiet)
        return success
