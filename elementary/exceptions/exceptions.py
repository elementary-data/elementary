class Error(Exception):
    """Base class for exceptions in this module."""


class ConfigError(Error):
    """Exception raised for errors in configuration"""


class SerializationError(Error):
    """Exception raised for errors during serialization / deserialization"""


class InvalidAlertType(Error):
    """Exception raised for unknown alert types in alerts table"""
