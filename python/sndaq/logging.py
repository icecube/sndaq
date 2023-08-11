"""Module for confiugring PySNDAQ's logging facilities
"""
import logging
import os
import shutil
import time

from sndaq import base_path

log_path = os.path.join(base_path, 'log')
log_file = os.path.join(log_path, 'pysndaq.log')
log_level = logging.DEBUG  # TODO: Set this up with config, and add option to disable logging
_logger_name = 'pysndaq'


def _rotator(source, dest):
    """Rotator for Log File"""
    with open(source, 'rb') as fin:
        with open(dest, 'wb') as fout:
            shutil.copyfileobj(fin, fout)
    os.remove(source)


# Check if logger currently exists, if not, create it
if _logger_name not in logging.Logger.manager.loggerDict.keys():
    rh = logging.handlers.RotatingFileHandler(log_file, backupCount=5)
    rh.rotator = _rotator

    logger = logging.getLogger(_logger_name)
    logger.setLevel(log_level)
    logger.addHandler(rh)

    # If a previous log file is found, rollover to a new file
    if os.path.isfile(log_file):
        logger.debug(f'\n---------\nLog closed on {time.asctime()}.\n---------\n')
        logger.handlers[0].doRollover()

else:
    logger = logging.getLogger(_logger_name)


#
# logging.basicConfig(filename='./pysndaq.log',
#                     filemode='a',
# #                     level=logging.DEBUG)
# logger = logging.getLogger('pysndaq')
