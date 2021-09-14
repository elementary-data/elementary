import os
import logging
import sys

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
LOG_FILE = "edl.log"


def is_flight_mode_on() -> bool:
    return is_env_var_on('FLIGHTMODE')


def is_debug_mode_on() -> bool:
    return is_env_var_on('DEBUG')


def is_env_var_on(env_var) -> bool:
    if os.getenv(env_var) == '1':
        print(env_var, ' is on!')
        return True

    return False


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    log_level = logging.DEBUG if is_debug_mode_on() else logging.INFO
    logger.setLevel(log_level)
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger
