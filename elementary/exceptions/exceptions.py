import subprocess
from typing import List

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


class NoProfilesFileError(ConfigError):
    """Exception raised if profiles.yml was not found"""

    def __init__(self, profiles_dir: str):
        super().__init__(
            f'Could not find "profiles.yml" at "{profiles_dir}". {_QUICKSTART_CLI_ERR_MSG}'
        )


class NoElementaryProfileError(ConfigError):
    """Exception raised if an 'elementary' profile doesn't exist"""

    def __init__(self):
        super().__init__(
            f'Unable to find "elementary" profile. {_QUICKSTART_CLI_ERR_MSG}'
        )


class InvalidArgumentsError(ConfigError):
    """Exception raised if user provided invalid arguments to the command"""

    @property
    def anonymous_tracking_context(self):
        return {
            # Messages for this exception are generic and don't contain sensitive information
            "exception_message": str(self)
        }


class SerializationError(Error):
    """Exception raised for errors during serialization / deserialization"""


class InvalidAlertType(Error):
    """Exception raised for unknown alert types in alerts table"""


class DbtCommandError(Error):
    """Exception raised while executing a dbt command"""
    def __init__(self, err: subprocess.CalledProcessError, command_args: List[str]):
        super().__init__(
            f"Failed to run dbt command - cmd: {err.cmd}, output: {err.output}, err: {err.stderr}"
        )

        self.command_args = command_args
        self.return_code = err.returncode

    @property
    def anonymous_tracking_context(self):
        return {
            "command_args": self.command_args,
            "return_code": self.return_code
        }
