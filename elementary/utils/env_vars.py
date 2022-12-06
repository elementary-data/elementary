import os


def is_debug() -> bool:
    return "DEBUG" in os.environ
