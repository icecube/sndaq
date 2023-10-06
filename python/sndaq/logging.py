"""Module for confiugring PySNDAQ's logging facilities
"""
import sys

from sndaq import base_path

import logging
from logging.handlers import RotatingFileHandler

import os
import shutil

from sndaq import base_path

_default_log_path = os.path.join(base_path, 'log')
_default_log_file = os.path.join(_default_log_path, 'pysndaq.log')
_default_log_level = logging.DEBUG  # TODO: Set this up with config, and add option to disable logging
_default_logger_name = 'pysndaq'


def _mod_func_filter(record):
    """Custom Filter function which adds context log messages, prepending the module and function (if applicable)
    from which the log message was issued.

    Parameters
    ----------
    record : logging.LogRecord

    Returns
    -------
    updated_record : logging.LogRecord

    Notes
    -----
    Python Logging Filter Documentation:
    <https://docs.python.org/3/howto/logging-cookbook.html#imparting-contextual-information-in-handlers>
    """

    # Using sys._getframe() may not be the best form, but the improved clarity in log messages is worth it.
    # Progression through frames (argument of sys._getframe()) proceeds as follows:
    # 0 (Default) : this function
    # 1--6        : Python `logging` functions (5 is logging._log(), 6 is log-level e.g. logger.error())
    # 7           : Function (or module) in which log message was issued.
    # See `inspect` library for details on attributes f_code (code object being executed in the frame) and
    #   co_name (name of code object)
    func_name = sys._getframe(7).f_code.co_name
    # If the log message was generated from a module (without a calling function) report only the module
    mod_func_str = f"{record.module}" if func_name == "<module>" else f"{record.module}.{func_name}"
    record.msg = f"{mod_func_str:>18s} | {record.msg}"
    return record


def _loglevel_fmt_filter(record):
    """Custom Filter function which formats the log-level name of log records.
    Parameters
    ----------
    record : logging.LogRecord

    Returns
    -------
    updated_record : logging.LogRecord"""
    record.levelname = f"{'['+record.levelname+']':>9s}"
    return record


def _rotator(source, dest):
    """Rotator for Log File"""
    with open(source, 'rb') as fin:
        with open(dest, 'wb') as fout:
            shutil.copyfileobj(fin, fout)
    os.remove(source)


def get_logger(name=_default_logger_name, log_path=_default_log_path, log_level=_default_log_level):
    """Get SNDAQ-style Logger singleton. If none exist, create a new one.
    Every time a new logger is created, the old logfile is rolled over.

    Parameters
    ----------
    name : str
        logger name (also sets logfile name)
    log_path : str or PathLike

    log_level : int
        Python Logging level e.g. logging.debug (10), logging.error (40)

    Returns
    -------
    logger : logging.Logger
        Python Logger configured with PySNDAQ format and style.
    """

    log_file = os.path.join(log_path, f'{name}.log')

    # Check if logger currently exists, if not, create it
    if name not in logging.Logger.manager.loggerDict.keys():
        handler = RotatingFileHandler(log_file, backupCount=3)
        handler.rotator = _rotator
        formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%S')
        handler.setFormatter(formatter)
        handler.addFilter(_mod_func_filter)
        handler.addFilter(_loglevel_fmt_filter)

        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        logger.addHandler(handler)

        # If a previous log file is found, rollover to a new file
        if os.path.isfile(log_file):
            logger.debug(f'Log File has been closed')
            logger.handlers[0].doRollover()

    else:
        logger = logging.getLogger(name)

    return logger


