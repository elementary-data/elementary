import subprocess

_QUICKSTART_CLI_ERR_MSG = (
    "Please refer for guidance - https://docs.elementary-data.com/quickstart-cli"
)


class Error(Exception):
    """Base class for exceptions in this module."""


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


class SerializationError(Error):
    """Exception raised for errors during serialization / deserialization"""


class InvalidAlertType(Error):
    """Exception raised for unknown alert types in alerts table"""


class DbtCommandError(Error):
    """Exception raised while executing a dbt command"""

    def __init__(self, err: subprocess.CalledProcessError):
        super().__init__(f"Failed to run dbt command - {err.cmd}")
