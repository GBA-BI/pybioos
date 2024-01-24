import logging

import click
from colorama import Fore


class Logger:
    _ERROR_LEVEL = 30
    _WARNING_LEVEL = 20
    _INFO_LEVEL = 10
    _DEBUG_LEVEL = 0

    _nameToLevel = {
        'ERROR': _ERROR_LEVEL,
        'WARN': _WARNING_LEVEL,
        'INFO': _INFO_LEVEL,
        'DEBUG': _DEBUG_LEVEL,
    }

    DEFAULT_LOGGER_LEVEL = _INFO_LEVEL
    _CUR_LEVEL = DEFAULT_LOGGER_LEVEL

    @classmethod
    def _check_level(cls, level):
        return cls._CUR_LEVEL <= level

    @classmethod
    def set_level(cls, level):
        if isinstance(level, int):
            cls._CUR_LEVEL = level
        elif str(level) == level:
            if level not in cls._nameToLevel:
                raise ValueError("Unknown level: %r" % level)
            cls._CUR_LEVEL = cls._nameToLevel[level]
        else:
            raise TypeError("Level not an integer or a valid string: %r" %
                            level)
        return cls._CUR_LEVEL

    @classmethod
    def debug(cls, content):
        pass

    @classmethod
    def info(cls, content):
        pass

    @classmethod
    def warn(cls, content):
        pass

    @classmethod
    def error(cls, content):
        pass


# TODO will be used for cli in the future
class ClickLogger(Logger):

    @classmethod
    def debug(cls, content):
        if cls._check_level(cls._DEBUG_LEVEL):
            click.secho(f"[DEBUG]:{content}", fg="green")

    @classmethod
    def info(cls, content):
        if cls._check_level(cls._INFO_LEVEL):
            click.secho(f"[INFO]:{content}")

    @classmethod
    def warn(cls, content):
        if cls._check_level(cls._WARNING_LEVEL):
            click.secho(f"[WARN]:{content}", fg="yellow")

    @classmethod
    def error(cls, content):
        if cls._check_level(cls._ERROR_LEVEL):
            click.secho(f"[ERROR]{content}", fg="red")


class PyLogger(Logger):

    class CustomFormatter(logging.Formatter):

        reset = "\x1b[0m"
        format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        FORMATS = {
            logging.DEBUG: Fore.GREEN + format + reset,
            logging.INFO: Fore.LIGHTWHITE_EX + format + reset,
            logging.WARNING: Fore.YELLOW + format + reset,
            logging.ERROR: Fore.RED + format + reset,
        }

        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno)
            formatter = logging.Formatter(log_fmt)
            return formatter.format(record)

    name = "bioos-sdk"
    logger = logging.getLogger(name)
    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    @classmethod
    def debug(cls, content):
        if cls._check_level(cls._DEBUG_LEVEL):
            cls.logger.debug(content)

    @classmethod
    def info(cls, content):
        if cls._check_level(cls._INFO_LEVEL):
            cls.logger.info(content)

    @classmethod
    def warn(cls, content):
        if cls._check_level(cls._WARNING_LEVEL):
            cls.logger.warning(content)

    @classmethod
    def error(cls, content):
        if cls._check_level(cls._ERROR_LEVEL):
            cls.logger.error(content)
