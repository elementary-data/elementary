class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class ConfigError(Error):
    """Exception raised for errors in configuration"""
    def __init__(self, message):
        self.message = message


class SerializationError(Error):
    """Exception raised for errors during serialization / deserialization"""
    def __init__(self, message):
        self.message = message


class InvalidAlertType(Error):
    """Exception raised for unknown alert types in alerts table"""
    def __init__(self, message):
        self.message = message
