"""PySNDAQ Main Function"""

import numpy as np
from sndaq.analysis import AnalysisHandler, AnalysisConfig
from sndaq.filehandler import FileHandler
from sndaq.datahandler import DataHandler
from sndaq.trigger import TriggerHandler
from sndaq.detector import Detector
from sndaq.communication import LiveMessageSender, get_unique_id
from sndaq import base_path

from multiprocessing import Process
import os

from sndaq.logging import logger


def launch(*args, **kwargs):
    logger.debug(f"Launching SNDAQ with the following Configuration:\nargs:{args}\nkwargs:{kwargs}")
    proc = Process(target=main, args=args, kwargs=kwargs)
    logger.info(f"== START ==")
    proc.start()


def main(*args, **kwargs):
    # TODO: Cleanup from spts-test
    logger.info(f"Creating LiveMessageSender sending to expcont:6668")
    lms = LiveMessageSender(moni_host='expcont', moni_port=6668)

    request_id = get_unique_id() if 'request_id' not in kwargs else kwargs['request_id']
    logger.debug(f"Registered FRA request: {request_id}")

    lms.fra_status(status='QUEUED', request_id=request_id)
    try:

        # === Setup core components ===
        # == Analysis ==
        # If another conf is not provided, use the defaults
        if not any([conf not in kwargs for conf in ('ana_conf', 'ana_conf_path')]):
            ana_conf_path = os.path.join(base_path, "data/config/analysis.config")
            ana_config = AnalysisConfig.from_config(conf_path=ana_conf_path)
        # A config object takes priority over a config file
        elif 'ana_conf' in kwargs:
            ana_config = kwargs['ana_conf']
        else:
            ana_config = AnalysisConfig.from_config(conf_path=kwargs['ana_conf_path'])
        ana = AnalysisHandler(ana_config)

        # == FileHandler ==
        # If a config is not provided, use the default
        if 'fh_conf_path' not in kwargs:
            fh_conf_path = os.path.join(base_path, "data/config/cobalt_test.config")
        else:
            fh_conf_path = kwargs['fh_conf_path']
        fh = FileHandler.from_config(conf_path=fh_conf_path)

        alert = TriggerHandler()
        dh = DataHandler()
        i3 = Detector(os.path.join(base_path, "data/config/full_dom_table.txt"))

        lms.fra_status(status='IN PROCESS', request_id=lms.request_id)

        if 'no_run_mode' in kwargs:
            if kwargs['no_run_mode']:
                raise Exception('SNDAQ was launched in `no_run_mode`, execution was automatically aborted.')

        start_time = kwargs['start_time'] if 'start_time' in kwargs else None
        stop_time = kwargs['stop_time'] if 'stop_time' in kwargs else None

        # Main SNDAQ Loop
        dh.get_scaler_files(fh.dir_scaler, start_time, stop_time)
        for file in dh.scaler_files:
            print(f'Processing {file}')
            dh.set_scaler_file(file)
            # Process file
            # Setup variables from first payload in file
            dh.read_payload()
            while not i3.isvalid_dom(dh.payload.dom_id):
                dh.read_payload()

            utime = dh.payload.utime
            dh._file_start_utime = utime
            if dh._start_utime is None and utime is not None:
                dh._start_utime = utime
                dh._raw_utime = np.arange(utime, utime + (dh._raw_udt * dh._staging_depth), dh._raw_udt)

            while dh.payload is not None:

                # Add to the current 2ms bin until it has filled...
                while dh.payload is not None and dh.payload.utime <= dh._raw_utime[1]:
                    idx_dom = i3.get_dom_idx(dh.payload.dom_id)
                    dh.update_buffer(idx_dom)  # Ensures first payload is added to buffer
                    dh.read_payload()

                    while dh.payload is not None and not i3.isvalid_dom(dh.payload.dom_id):  # Skip IceTop
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
                    ana.cand_count += 1
                    alert.process_trigger(ana.current_trigger)
                    ana.current_trigger.reset()
                    ana.trigger_count = 0
                    print('')

            # If payload is none, then EOF reached. Close file and move to next one
            dh._scaler_file.close()

        # TODO Move logger messages into function, main shouldn't be too cluttered
        logger.info(f'FRA Request {lms.request_id} Completed')
        lms.fra_result(data={'PLACEHOLDER_KEY': 'PLACEHOLDER_VALUE'}, request_id=lms.request_id)

        logger.info(f'Results sent to live ({lms.request_id})')
        lms.fra_status(status='SUCCESS', request_id=lms.request_id)
        logger.info(f'Live status marked as \'SUCCESS\' ({lms.request_id})')

    except KeyboardInterrupt as e:
        logger.error(str(e))
        lms.fra_status('ERROR', lms.request_id)
        lms.sender.send_moni(varname='sndaq_fra_error', prio=2, value={'request_id': lms.request_id, "value": "Manual Abort"})
    except Exception as e:
        logger.error(str(e))
        lms.fra_status('ERROR', lms.request_id)
        lms.sender.send_moni(varname='sndaq_fra_error', prio=2, value={"request_id": lms.request_id, "err_msg": str(e)})
    finally:

        logger.info('SNDAQ Shutting Down')
        logger.info('Closing ZMQMoniClient')
        # lms.sender.close()
        logger.info('=== STOP ===')


