import logging
import sys
from logging.handlers import RotatingFileHandler

from utils.env_vars import is_debug_mode_on

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = "edr.log"


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    console_handler.setLevel(logging.DEBUG if is_debug_mode_on() else logging.INFO)
    return console_handler


def get_file_handler():
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=3)
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
