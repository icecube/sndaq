"""PySNDAQ Main Function"""

import configparser as cp
import numpy as np
from sndaq.analysis import AnalysisHandler, AnalysisConfig
from sndaq.filehandler import FileHandler
from sndaq.datahandler import DataHandler
from sndaq.detector import Detector

if __name__ == "__main__":

    # Setup core components
    ana_conf_path = "../../scratch/analysis.config"
    ana_config = AnalysisConfig.from_config(conf_path=ana_conf_path)
    ana = AnalysisHandler(ana_config)

    fh_conf_path = "../../scratch/default.config"
    fh_config = AnalysisConfig.from_config(conf_path=fh_conf_path)
    fh = FileHandler(fh_config)

    dh = DataHandler()
    i3 = Detector('../data/config/full_dom_table.txt')

    # Main SNDAQ Loop
    dh.get_scaler_files(fh.dir_scaler)
    for file in dh.scaler_files:
        print(f'Processing {file}')
        dh.set_scaler_file(file)  # TODO set up as context ("with dh(file) as current_file; current_file.read_payload()")
        ### Process file
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

            # Advance buffer after 2 ms bin is ready
            while dh.payload is not None and dh.payload.utime <= dh._raw_utime[1]:
                idx_dom = i3.get_dom_idx(dh.payload.dom_id)
                dh.update_buffer(idx_dom)  # Ensures first payload is added to buffer
                dh.read_payload()

                while dh.payload is not None and not i3.isvalid_dom(dh.payload.dom_id):  # Skip IceTop
                    dh.read_payload()

            if dh._pay is not None:
                # dh._pay is None only if EOF is reached, this ensures raw bins that span files
                # receive all contributing scalers.
                ana.update(dh._data[:, 0])
                # Eval trigger
                dh.advance_buffer()

        # If payload is none, then EOF reached. Close file
        dh._scaler_file.close()

    print(ana.config)


