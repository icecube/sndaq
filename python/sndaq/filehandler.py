"""Objects and functions for SNDAQ file management
"""
from configparser import ConfigParser
from pathlib import Path

import numpy as np
import glob
import os
import re
import ast  # TODO: Replace with pyyaml
import tarfile

from sndaq.logging import get_logger

logger = get_logger()

class FileHandler:
    """Handler for SNDAQ files
    """
    def __init__(self, dir_sndaq, run_info=None, dir_scaler=None, dir_scaler_bkp=None, dir_sndata=None, dir_config=None,
                 dir_log=None, dir_pdaq=None, dir_pdaq_trigger=None, dir_pdaq_trigger_bkp=None, dir_spade=None):
        """

        Parameters
        ----------
        dir_sndaq : str or path-like
            Path on system to root of SNDAQ running area. If any of the following arguments are relative paths
            they are assumed to be relative to this path.
        dir_scaler :  str or path-like
            Path to SN scaler data
        dir_scaler_bkp :  str or path-like
            Path to SN scaler data backup
        dir_log :  str or path-like
            Path to SNDAQ log files
        dir_config :  str or path-like
            Path to SNDAQ caonfig files
        dir_sndata : str or path-like
            Path to SN Data. May be used in place of input in place of `dir_scaler`
        dir_pdaq : str or path-like
            Path to pDAQ staging location on SP(T)S for unprocessed scalers
        run_info : dict
            i3Live run configuration specification
        dir_pdaq_trigger :  str or path-like
            Path to Muon rate data
        dir_pdaq_trigger_bkp :  str or path-like
            Path to Muon rate data backup
        dir_spade :  str or path-like
            Path to SPADE pickup on SP(T)S.

        """
        # TODO: Add Error checking
        # These are always required
        # TODO: Add checks that these are defined
        self.dir_sndaq = dir_sndaq
        self.dir_scaler = dir_scaler
        self.dir_scaler_bkp = dir_scaler_bkp
        self.dir_config = dir_config
        self.dir_sndata = dir_sndata

        # These are optional, but usually defined
        # TODO: Add checks to ensure setup with optional configuration
        self.dir_config = dir_config
        self.dir_pdaq = dir_pdaq
        self.dir_log = dir_log
        if dir_pdaq_trigger:
            self.dir_pdaq_trigger = dir_pdaq_trigger
        else:
            self.dir_pdaq_trigger = './'
        # self.dir_pdaq_trigger = '/home/sgris/Code/IceCube/sndaq/pysndaq/scratch/data/pdaq_triggers'
        self.dir_pdaq_trigger_bkp = dir_pdaq_trigger_bkp
        self.dir_log = dir_log
        self.dir_config = dir_config
        self.dir_spade = dir_spade

        # This is provided at initialization
        self.run_info = run_info

        # Variables for file management
        self.scaler_files = None
        self.scaler_files_processed = []
        self.scaler_files_pattern = None
        # TODO: Add more selective regex for these
        # Example: SPS-pDAQ-2ndBld-000_20230621_111601_000000.sn.tar
        self.scaler_tarball_pattern = 'SPS-pDAQ-2ndBld-*_*_*_*.sn.tar'
        # Example: sn_260295_000431_133620063_133930082.dat
        self.scaler_data_pattern = 'sn_*_*_*_*.dat'
        self.pdaq_trigger_files = None
        if self.run_info:
            self.pdaq_trigger_pattern = os.path.join(self.dir_pdaq_trigger, f'pDaqTriggers_{self.run_info["year"]}_*.dat')
        else:
            import datetime
            # TODO: Add better handling for this, if runinfo was properly handled this would be unecessary
            self.pdaq_trigger_pattern = os.path.join(self.dir_pdaq_trigger,
                                                     f'pDaqTriggers_{datetime.datetime.now().year}_*.dat')
        # TODO: Add checks for variable data sources

    @classmethod
    def from_config(cls, conf=None, conf_path=None):
        # TODO: Figure out how to avoid duplicating code here, perhaps make this an ABC or inherited method?
        """Initialize FileHandler from Config or Config file

        Parameters
        ----------
        conf : configparser.ConfigParser
            Analysis configuration
        conf_path :
            Path to file containing analysis configuration
        """
        if conf is None and conf_path is None:
            raise ValueError("Missing configuration")
        elif conf is None and conf_path is not None:
            if not os.path.exists(conf_path):
                err_msg = f"Unable to find config file: {conf_path}"
                logger.error(err_msg)
                raise RuntimeError(err_msg)
            conf = ConfigParser()
            conf.read(conf_path)

        conf_dict = {key: ast.literal_eval(val) for key, val in conf['filesystem'].items()}
        try:
            return cls(**conf_dict)
        except TypeError as err:
            msg = str(err)
            bad_field = msg.split('\'')[-2]
            if "required positional argument" in msg:
                raise TypeError(f"Config.: {conf} is missing a required field: '{bad_field}'") from err
            elif "got an unexpected keyword argument" in msg:
                raise TypeError(f"Config.: {conf} contains an unexpected field: '{bad_field}'") from err
            else:
                raise err

    def get_scalers_from_pdaq(self):
        """Searches `dir_pdaq` for new scaler files, grabs them, unpacks them, and moves scaler to `dir_scalers`
        """
        tarballs = np.array(glob.glob(self.scaler_tarball_pattern))
        for tarball in tarballs:
            with tarfile.open(tarball) as tf:
                # Get scaler data files within tarball
                scaler_data_files = [s for s in tf.getnames() if re.match(self.scaler_files_pattern, s)]
                for file in scaler_data_files:
                    # Write to SNDAQ scaler dir
                    with open(os.path.join(self.dir_scaler, file), 'wb') as f:
                        f.write(tf.extractfile(file).read())
        # TODO: Check if SNDAQ is responsible for removing Tarball after.

    def update_scaler_files(self):
        """Refresh scaler file list
        """
        self.scaler_files = np.array(glob.glob(self.scaler_files_pattern))

    def flush_processed_scaler_files(self):
        """Mark processed files with `.processed` and move to backup
        """
        for file in self.scaler_files_processed:
            # Move file to backup
            Path(os.path.join(self.dir_scaler, file)).rename(os.path.join(self.dir_scaler_bkp, file+'.processed'))
        self.scaler_files_processed = []

    def update_pdaq_trigger_files(self):
        """Refresh pDAQ Trigger file list
        """
        self.pdaq_trigger_files = np.array(glob.glob(self.pdaq_trigger_pattern))

    def get_cand_pdaq_triggers(self, cand):
        """Update candidate with associated pDAQ trigger files

        Parameters
        ----------
        cand : sndaq.trigger.Candidate
            SNDAQ Candidate Trigger

        Returns
        -------
        pdaq_trigger_files : np.ndarray of str
            List of pDAQ trigger file basenames

        """
        self.update_pdaq_trigger_files()
        rgx_timestamp = '([0-9]{4})(?:_)?([0-9]+)'
        timestamps = np.array([np.datetime64(year, 'Y') + np.timedelta64(dt_s, 's')
                               for year, dt_s in re.findall(rgx_timestamp, ''.join(self.pdaq_trigger_files))])
        diff = (cand.trigger.t - timestamps).astype(int)

        # TODO: change this s.t. it depends on the config, this assumes symmetrical time window
        n_after = 6
        n_before = 6

        # Get `n_before` files from before the trigger and `n_after` files from after the trigger
        idx_files = np.append(np.where(diff > 0)[0][-n_before:], np.where(diff < 0)[0][:n_after])

        # This is a modification in-place.
        # Python arguments are passed by reference, but that reference is passed by value.
        # Meaning, we may modify the object `cand` references if that object is mutable (it is).
        # However, we cannot change which object `cand` references.
        cand.rmu_files = list([os.path.basename(self.pdaq_trigger_files[idx]) for idx in idx_files])

    def flush_pdaq_trigger_files(self, cutoff):
        """Remove unneeded Muon rate files from the list of current files, and move to backup

        Parameters
        ----------
        cutoff : np.datetime64
            cutoff time, before which files are no longer needed
        """
        # TODO: put these line in their own function or figure out how to avoid duplicating this here
        # Get time difference between pdaq_trigger file start times and cutoff
        rgx_timestamp = '([0-9]{4})(?:_)?([0-9]+)'
        timestamps = np.array([np.datetime64(year, 'Y') + np.timedelta64(dt_s, 's')
                               for year, dt_s in re.findall(rgx_timestamp, ''.join(self.pdaq_trigger_files))])
        diff = (cutoff - timestamps).astype(int)

        for file in self.pdaq_trigger_files[diff < 0]:  # Where file start time is beyond cutoff
            # Move file to backup
            Path(os.path.join(self.dir_pdaq_trigger, file)).rename(os.path.join(self.dir_pdaq_trigger_bkp, file))


    def write_candidate(self):
        raise NotImplementedError
