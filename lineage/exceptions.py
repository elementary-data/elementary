class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class ConfigError(Error):
    """Exception raised for errors in the profiles configuration"""
    def __init__(self, message):
        self.message = message
