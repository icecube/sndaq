"""PySNDAQ Main Function"""

import numpy as np
from sndaq.analysis import AnalysisHandler, AnalysisConfig
from sndaq.filehandler import FileHandler
from sndaq.datahandler import DataHandler
from sndaq.trigger import TriggerHandler
from sndaq.detector import Detector
from sndaq.communication import LiveMessageSender

if __name__ == "__main__":
    lms = LiveMessageSender(moni_host='expcont', moni_port=6668)
    lms.fra_status(status='QUEUED', id=3)
    try:

        # Setup core components
        ana_conf_path = "../../data/config/analysis.config"
        ana_config = AnalysisConfig.from_config(conf_path=ana_conf_path)
        ana = AnalysisHandler(ana_config)

        fh_conf_path = "../../data/config/cobalt_test.config"
        fh = FileHandler.from_config(conf_path=fh_conf_path)

        alert = TriggerHandler()
        dh = DataHandler()
        i3 = Detector('../../data/config/full_dom_table.txt')


        lms.fra_status(status='IN PROCESS', id=3)
        # Main SNDAQ Loop
        dh.get_scaler_files(fh.dir_scaler)
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

        lms.fra_result(data={'PLACEHOLDER_KEY': 'PLACEHOLDER_VALUE'}, id=lms.request_id)
        lms.fra_status(status='SUCCESS', id=lms.request_id)

    except KeyboardInterrupt as e:
        lms.fra_status('ERROR', lms.request_id)
        lms.sender.send_moni(varname='sndaq_fra_error', prio=3, id=lms.request_id, value="Manual Abort")
    except Exception as e:
        lms.fra_status('ERROR', lms.request_id)
        lms.sender.send_moni(varname='sndaq_fra_error', prio=3, id=lms.request_id, value=str(e))
    finally:
        lms.sender.close()


