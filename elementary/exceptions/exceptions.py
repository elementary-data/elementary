import json
import subprocess
from typing import List

from elementary.utils.log import get_logger

logger = get_logger(__name__)

_QUICKSTART_CLI_ERR_MSG = (
    "Please refer for guidance - https://docs.elementary-data.com/quickstart-cli"
)


class Error(Exception):
    """Base class for exceptions in this module."""

    @property
    def anonymous_tracking_context(self):
        return {}


class ConfigError(Error):
    """Exception raised for errors in configuration"""


class InvalidArgumentsError(ConfigError):
    """Exception raised if user provided invalid arguments to the command"""

    @property
    def anonymous_tracking_context(self):
        return {
            # Messages for this exception are generic and don't contain sensitive information
            "exception_message": str(self)
        }


class DbtCommandError(Error):
    """Exception raised while executing a dbt command"""

    def __init__(
        self, err: subprocess.CalledProcessError, base_command_args: List[str]
    ):
        super().__init__(f"Failed to run dbt command - {vars(err)}")

        # Command args sent to _run_command (without additional user-specific args it as such as projects / profiles
        # dir)
        self.base_command_args = base_command_args
        self.return_code = err.returncode

    @property
    def anonymous_tracking_context(self):
        return {
            "command_args": self.base_command_args,
            "return_code": self.return_code,
            **self.extract_detailed_dbt_command_args(self.base_command_args),
        }

    @staticmethod
    def extract_detailed_dbt_command_args(command_args):
        try:
            dbt_command_type = command_args[0]
            detailed_command_args = {"dbt_command_type": dbt_command_type}

            if dbt_command_type == "run-operation":
                detailed_command_args["macro_name"] = command_args[1]

                if "--args" in command_args:
                    args_index = command_args.index("--args")
                    detailed_command_args["macro_args"] = json.loads(
                        command_args[args_index + 1]
                    )

            return detailed_command_args
        except Exception as ex:
            logger.error(f"Failed to extract detailed dbt command args, error: {ex}")
