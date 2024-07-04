import json
import subprocess
from typing import List, Optional

from elementary.clients.dbt.dbt_log import DbtLog
from elementary.utils.log import get_logger

logger = get_logger(__name__)


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
        self,
        base_command_args: List[str],
        err_msg: Optional[str] = None,
        logs: Optional[List[DbtLog]] = None,
        err: Optional[subprocess.CalledProcessError] = None,
    ):
        msg = "Failed to run dbt command."
        if logs and not err_msg:
            err_msg = "\n".join(
                [log.msg for log in logs if log.msg and log.level == "error"]
            )
        if err_msg:
            msg = f"{msg}\n{err_msg}"
        super().__init__(msg)

        # Command args sent to _run_command (without additional user-specific args it as such as projects / profiles
        # dir)
        self.proc_err = err
        self.base_command_args = base_command_args
        self.return_code = err.returncode if err else None
        self.logs = logs

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

    def get_exception_message(self) -> Optional[str]:
        if not self.logs:
            return None
        for log in reversed(self.logs):
            if log.exception:
                return log.exception
        return None


class DbtLsCommandError(Error):
    """Exception raised while executing a dbt ls command"""

    def __init__(self, selector: Optional[str] = None):
        self.selector = selector
        self.selector_method = self.extract_selector_method(self.selector)
        super().__init__(
            f"Failed to run dbt ls - '{self.selector_method}' is not a valid selector method!"
        )

    @property
    def anonymous_tracking_context(self):
        return {
            "dbt_selector_method": self.selector_method,
        }

    @staticmethod
    def extract_selector_method(selector: Optional[str] = None):
        if selector:
            try:
                return selector.split(":", 1)[0]
            except Exception:
                logger.error(f"Failed to extract selector method from: '{selector}'")


class UnsupportedSelectorError(Error):
    """Exception raised while executing edr command with unsupported --select method"""

    def __init__(self, selector: Optional[str] = None):
        self.selector = selector
        self.selector_method = self.extract_selector_method(self.selector)
        super().__init__(
            f"Failed to run edr command with `--select` - '{self.selector_method}' is not a valid selector method!\nFor using all of dbt selector methods, please provide --project-dir!"
        )

    @property
    def anonymous_tracking_context(self):
        return {
            "edr_selector_method": self.selector_method,
        }

    @staticmethod
    def extract_selector_method(selector: Optional[str] = None):
        if selector:
            try:
                return selector.split(":", 1)[0]
            except Exception:
                logger.error(f"Failed to extract selector method from: '{selector}'")
