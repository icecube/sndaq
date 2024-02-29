"""PySNDAQ Main Function"""

import numpy as np
from sndaq.analysis import AnalysisHandler, AnalysisConfig
from sndaq.filehandler import FileHandler
from sndaq.datahandler import DataHandler
from sndaq.trigger import TriggerHandler
from sndaq.detector import Detector
from livecore.util.misc import exc_string
from sndaq.communication import LiveMessageSender, get_unique_id
from sndaq import base_path
from sndaq.util import utime_to_datetime64

from multiprocessing import Process
import os
import time

from sndaq.logging import get_logger

logger = get_logger()


def launch(*args, **kwargs):
    """Launches SNDAQ instance
    """
    logger.debug(f"Launching SNDAQ with the following Configuration:\nargs:{args}\nkwargs:{kwargs}")
    proc = Process(target=main, args=args, kwargs=kwargs)
    logger.info(f"== START ==")
    proc.start()
    proc.join(timeout=5)
    while proc.is_alive():
        time.sleep(5)
    logger.info("Process Ended")


def main(*args, **kwargs):
    # TODO: Cleanup from spts-test
    logger.info(f"Creating LiveMessageSender sending to expcont:6668")
    lms = LiveMessageSender(moni_host='expcont', moni_port=6668, msg=kwargs['msg'], offline=kwargs['offline_mode'])
    request_id = get_unique_id() if 'request_id' not in kwargs else kwargs['request_id']
    logger.debug(f"Registered FRA request: {request_id}")

    lms.fra_status(status='QUEUED', request_id=request_id)
    try:

        # === Setup core components ===
        # == Analysis ==
        # If another conf is not provided, use the defaults
        if not any([conf not in kwargs for conf in ('ana_conf', 'ana_conf_path')]):
            ana_conf_path = os.path.join(base_path, "etc/analysis.ini")
            ana_config = AnalysisConfig.from_config(conf_path=ana_conf_path)
        # A config object takes priority over a config file
        elif 'ana_conf' in kwargs:
            ana_config = kwargs['ana_conf']
        else:
            ana_config = AnalysisConfig.from_config(conf_path=kwargs['ana_conf_path'])
        ana = AnalysisHandler(ana_config)  # ADD DROPPED DOMS!!!

        # == FileHandler ==
        # If a config is not provided, use the default
        if 'conf_path' not in kwargs:
            conf_path = os.path.join(base_path, "etc/default.ini")
        else:
            conf_path = kwargs['conf_path']
        fh = FileHandler.from_config(conf_path=conf_path)

        start_time = kwargs['start_time']
        stop_time = kwargs['stop_time'] if 'stop_time' in kwargs else None
        if stop_time is None:
            stop_time = np.datetime64(start_time) + np.timedelta64(ana.config.base_binsize, 'ms')

        assert (start_time is not None)
        start_time = np.datetime64(start_time)

        # Only applicable to FastResponseTriggers
        ana.config._trigger_condition.set_trigger_time(np.datetime64(kwargs['start_time']))
        alert = TriggerHandler(primary_trigger=ana.config._trigger_condition)
        dh = DataHandler.from_config(conf_path=conf_path)
        i3 = Detector(os.path.join(base_path, "etc/full_dom_table.txt"))

        lms.fra_status(status='IN PROGRESS', request_id=lms.request_id)

        if 'no_run_mode' in kwargs:
            if kwargs['no_run_mode']:
                raise Exception('SNDAQ was launched in `no_run_mode`, execution was automatically aborted.')


        # TODO Figure out a better way of handling this for FR
        result_dict = {'xi': {},
                       'lightcurve': {}}

        # Main SNDAQ Loop
        # TODO: Clean this up!
        dh.get_scaler_files(fh.dir_scaler, start_time, stop_time,
                            ana_config.duration_bgl_ms, ana_config.duration_bgt_ms)

        for i, file in enumerate(dh.scaler_files):
            logger.debug(f'Processing {file}')
            dh.set_scaler_file(file)
            # Process file
            # Setup variables from first payload in file
            dh.read_payload()
            while not i3.isvalid_dom(dh.payload.dom_id):
                dh.read_payload()

            utime = dh.payload.utime
            dh._file_start_utime = utime
            if dh._start_utime is None and utime is not None:  # First file opening in processing run
                dh._start_utime = utime
                ana.set_start_time(utime_to_datetime64(utime, year=start_time.item().year))
                dh._raw_utime = np.arange(utime, utime + (dh._raw_udt * dh._staging_depth), dh._raw_udt)

            while dh.payload is not None and ana.trigger_time() < stop_time:

                # Add to the current 2ms bin until it has filled...
                while dh.payload is not None and dh.payload.utime <= dh._raw_utime[1]:

                    # Skip IceTop
                    if not i3.isvalid_dom(dh.payload.dom_id):
                        dh.read_payload()
                        continue

                    idx_dom = i3.get_dom_idx(dh.payload.dom_id)
                    dh.update_buffer(idx_dom)  # Ensures first payload is added to buffer
                    dh.read_payload()

                # Then add the filled 2ms bin to raw analysis buffer
                if dh._pay is not None:
                    # dh._pay is None only if EOF is reached
                    # When this line is reached, and dh._pay is None, the current scaler file ended before
                    # The filling of the current bin ended. Wait for the next file
                    # TODO: Figure out a better way of handling this

                    # Adds 2ms data to Raw analysis buffer and accumulator for 500ms buffer
                    # If 500 ms of data has accumulated, this will also update the analyses
                    ana.update(dh._data.front)
                    dh.advance_buffer()

                # If a trigger has been finalized, process it.
                if ana.trigger_finalized:
                    ana.cand_count += len(ana.candidates)
                    for cand in ana.candidates:
                        alert.process_cand(cand)
                        # update if any of the following conditoions are met:
                        # 1 - Result dict is empty of lightcurves
                        # 2 - Result Dict has a lightcurve, but in a different binning
                        # 3 - The current candidate has a higher TS in a seach with the same binsize
                        if (not result_dict['lightcurve'] or
                            not result_dict['lightcurve'][str(cand.binsize)] or
                                result_dict['lightcurve'][str(cand.binsize)]['xi'] < cand.xi):
                            result_dict['lightcurve'].update({
                                str(cand.binsize):
                                    {'data': ana.get_avg_lightcurve(cand.ana,
                                                                    int(kwargs['lightcurve'][0]),
                                                                    int(kwargs['lightcurve'][1])),
                                     'offset_ms': int(kwargs['lightcurve'][0] % cand.binsize)}
                            })
                            result_dict['xi'].update(
                                {str(cand.binsize): {'value': cand.ana.xi}}
                            )
                    ana.candidates = []
                    ana.trigger_count = 0

            # If payload is none, then EOF reached. Close file and move to next one
            dh._scaler_file.close()

        # TODO Move logger messages into function, main shouldn't be too cluttered
        logger.info(f'FRA Request {lms.request_id} Completed')
        lms.fra_result(request_id=lms.request_id, data=result_dict)

        logger.info(f'Results sent to live ({lms.request_id})')
        logger.info(f'Live status marked as \'SUCCESS\' ({lms.request_id})')

    except KeyboardInterrupt as e:
        logger.error(str(e))
        lms.fra_status('FAIL', lms.request_id)
        lms.sender.send_moni(varname='sndaq_fra_info', prio=2, value={'request_id': lms.request_id,
                                                                      "value": "Manual Abort"})
    except Exception as e:
        logger.error(exc_string())
        lms.fra_status('FAIL', lms.request_id)
        lms.sender.send_moni(varname='sndaq_fra_info', prio=2, value={"request_id": lms.request_id,
                                                                      "err_msg": exc_string()})
    finally:
        logger.info('SNDAQ Shutting Down')
        logger.info('Closing ZMQMoniClient')
        # lms.sender.close()
        logger.info('=== STOP ===')
