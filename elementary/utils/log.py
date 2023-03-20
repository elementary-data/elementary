import logging
import sys

from elementary.utils.env_vars import is_debug


class ColoredFormatter(logging.Formatter):
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"
    FORMAT = "%(asctime)s — %(levelname)s — %(message)s"
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    FORMATS = {
        logging.WARNING: YELLOW + FORMAT + RESET,
        logging.ERROR: RED + FORMAT + RESET,
        logging.CRITICAL: BOLD_RED + FORMAT + RESET,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.FORMAT)
        formatter = logging.Formatter(log_fmt, self.DATE_FORMAT)
        return formatter.format(record)


FORMATTER = ColoredFormatter()


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    console_handler.setLevel(logging.DEBUG if is_debug() else logging.INFO)
    return console_handler


def get_file_handler(files_target_path):
    file_handler = logging.FileHandler(files_target_path, delay=True)
    file_handler.setFormatter(FORMATTER)
    file_handler.setLevel(logging.DEBUG)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    return logger


def set_root_logger_handlers(logger_name, files_target_path):
    logger = logging.getLogger(logger_name)
    logger.addHandler(get_file_handler(files_target_path))
    logger.addHandler(get_console_handler())
