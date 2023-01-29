import os


def is_debug() -> bool:
    return "EDR_DEBUG" in os.environ
