import logging
import sys

from elementary.utils.env_vars import is_debug

FORMATTER = logging.Formatter(
    "%(asctime)s — %(levelname)s — %(message)s", "%Y-%m-%d %H:%M:%S"
)
LOG_FILE = "edr.log"


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    console_handler.setLevel(logging.DEBUG if is_debug() else logging.INFO)
    return console_handler


def get_file_handler():
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(FORMATTER)
    file_handler.setLevel(logging.DEBUG)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger
