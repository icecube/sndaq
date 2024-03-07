"""Objects and functions for handling and processing raw data used by SNDAQ
"""
from sndaq.reader import SN_PayloadReader, PDAQ_PayloadReader
from sndaq.buffer import stagingbuffer
from sndaq.util.rebin import rebin_scalers as c_rebin_scalers
from sndaq.util import utime_to_datetime64
from sndaq.logger import get_logger
from sndaq.communication import RunInfoAgent
from sndaq import get_i3creds

from configparser import ConfigParser
import numpy as np
import glob
import os
import ast  # TODO: Replace with pyyaml

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

logger = get_logger()
_i3user, _i3pass = get_i3creds()


class DataHandler:
    """Handler for SN scaler data files
    """
    def __init__(self, ndom=5160, dtype=np.uint16, livehost=None, *, run_number=None):
        """Create DataHandler object

        Parameters
        ----------
        ndom : int
            Number of contributing DOMs
        dtype
            Data type for SN scaler arrays
        """

        self._scaler_udt = int(250 * 2**16)
        self._scaler_dt = self.scaler_udt / 1e7
        self._raw_dt = 2
        self._raw_udt = int(self._raw_dt * 1e7)
        self._staging_depth = 4000
        self._payloads_read = np.zeros(5160, dtype=np.uint32)

        self._data = stagingbuffer(size=self._staging_depth, ndom=ndom, dtype=dtype)  #np.zeros((ndom, self._staging_depth), dtype=dtype)
        self._raw_utime = np.zeros(self._staging_depth, dtype=np.uint32)

        self._scaler_file = None
        self._scaler_file_glob = None
        self._pay = None
        self._start_utime = None
        self._file_start_utime = None

        self._pdaqtrigger_file = None
        self._pdaqtrigger_file_glob = None

        # TODO: Host must be configurable
        self._run_info_agent = RunInfoAgent(host=livehost, force=False, run_number=run_number)
        self._run_number = run_number

    @classmethod
    def from_config(cls, conf=None, conf_path=None):
        # TODO: Put non-specific conf loading in sndaq function _load_config, use to decorate specific functions
        #       that perform processing action for sndaq classes that perform processing to return conf_dict
        """Initialize DataHandler from Config or Config file

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

        # Default arguments specified in init
        livehost = ast.literal_eval(conf['i3live'].get('host', None))
        run_number = conf['i3live'].getint('run_number', None)

        try:
            return cls(livehost=livehost, run_number=run_number)
        except TypeError as err:
            msg = str(err)
            bad_field = msg.split('\'')[-2]
            if "required positional argument" in msg:
                raise TypeError(f"Config.: {conf} is missing a required field: '{bad_field}'") from err
            elif "got an unexpected keyword argument" in msg:
                raise TypeError(f"Config.: {conf} contains an unexpected field: '{bad_field}'") from err
            else:
                raise err

    @property
    def scaler_files(self):
        """List of SN scaler file paths

        Returns
        -------
        files : list
            Paths to files containing unprocessed sn scaler data
        """
        return self._scaler_file_glob

    @property
    def pdaqtrigger_files(self):
        """List of SN scaler file paths

        Returns
        -------
        files : list
            Paths to files containing unprocessed sn scaler data
        """
        return self._pdaqtrigger_file_glob

    def get_scaler_files(self, directory, start_time, stop_time=None, buffer_time_l=None, buffer_time_t=None,
                         scaler_file_pattern='sn_{run_number}*.dat'):
        # TODO: Move to file handler
        """Get SN scaler files from a directory

        Parameters
        ----------
        directory : str | os.PathLike
            Directory to search for SN data files
        start_time : str
            date_time string for start of search
        stop_time : str
            date_time string for end of search
        buffer_time_l : int
            Amount of time for leading buffer in ms
        buffer_time_t : int
            Amount of time for trailing buffer in ms
        scaler_file_pattern : str

        """
        # TODO: Move to Init so this isn't performed multiple times
        if not os.path.exists(directory):
            err_msg = f"Requested scaler directory '{directory}' does not exist! "
            logger.error(err_msg)
            raise RuntimeError(err_msg)

        start_datetime = np.datetime64(start_time)
        stop_datetime = np.datetime64(stop_time) if stop_time is not None else start_datetime

        if not self._run_number:
            # Get Run Info
            self._run_number = self._run_info_agent.find_run_number(start_datetime)
            # run_info = self._run_info_agent.get_run_info(self._run_number)
            # run_start = np.datetime64(run_info['start'])

        # Find Corresponding Scaler files
        self._scaler_file_glob = sorted(
            glob.glob('/'.join((directory, scaler_file_pattern.format(run_number=self._run_number)))))

        logger.debug(f"Searching for files matching pattern {'/'.join((directory, scaler_file_pattern))}")
        logger.debug(f"start_time={start_time} stop_time={stop_time}")

        # Estimate scaler file times
        idc = np.array([int(os.path.basename(file).split('_')[2]) for file in self._scaler_file_glob])

        # TODO: Revisit getting file times based on run start time
        # if self._use_real_run_no:
        #     file_times = np.timedelta64(60, 's') * idc + run_start  # Each file is about a minute
        # else:
        self.set_scaler_file(self._scaler_file_glob[0])
        self.read_payload()
        data_start = utime_to_datetime64(self.payload.utime, start_time.astype('datetime64[Y]').item().year)
        file_times = np.timedelta64(60, 's') * idc + data_start

        # search between [t_sw - (t_bkg_t + 1 min), t_sw + t_bkg_l + 1min], extra 4 min to ensure file selection
        t0 = start_datetime - (np.timedelta64(buffer_time_t, 'ms') + np.timedelta64(2, 'm'))
        t1 = stop_datetime + (np.timedelta64(buffer_time_l, 'ms') + np.timedelta64(2, 'm'))

        logger.debug(
            f"start_datetime={start_datetime} stop_datetime={stop_datetime} buffer_time_t={buffer_time_t} buffer_time_l={buffer_time_l}")
        logger.debug(f"Searching between times {t0} {t1} , found file times {file_times}")

        # Select files according to specified time window
        idx_glob = np.where((t0 < file_times) & (file_times < t1))[0]
        self._scaler_file_glob = np.array(self._scaler_file_glob)[idx_glob]

        if len(self._scaler_file_glob) == 0:
            raise Exception("No Scaler files found!")
        logger.debug(f"Found files: {self._scaler_file_glob}")

    def get_pdaqtrigger_files(self, directory):
        # TODO: Move to file handler
        """Get pDAQ trigger files from a directory

        Parameters
        ----------
        directory : str | os.PathLike
            Directory to search for pDAQ files
        """
        self._pdaqtrigger_file_glob = sorted(glob.glob('/'.join((directory, 'pdaqtriggers*.dat'))))

    def set_scaler_file(self, filename):
        """Set current SN scaler data file

        Parameters
        ----------
        filename : str | os.PathLike
            String or path of SN Scaler data file
        """
        self._scaler_file = SN_PayloadReader(filename)

    def set_pdaqtrigger_file(self, filename):
        """Set current SN scaler data file

        Parameters
        ----------
        filename : str | os.PathLike
            String or path of SN Scaler data file
        """
        self._pdaqtrigger_file = PDAQ_PayloadReader(filename)

    def read_payload(self):
        """Read one SN scaler payload from the current file

        See Also
        --------
        sndaq.reader.SN_PayloadReader
        sndaq.reader.SN_Payload

        """
        self._pay = next(self._scaler_file)

    @property
    def payload(self):
        """Current SN scaler payload

        Returns
        -------
        payload : sndaq.reader.SN_Payload
            SN scaler payload

        See Also
        --------
        sndaq.reader.SN_Payload
        """
        return self._pay

    @property
    def next_scalers(self):  # TODO: Figure out a better name
        """Retrieve the first column of scaler data from staging buffer

        Returns
        -------
        scalers : numpy.ndarray
            ndom-length array on SN scaler data from first column of staging buffer
        """
        return self._data[:, 0]

    def advance_buffer(self):
        """Roll front of staging buffer off the end, add empty space at back
        """
        self._raw_utime += self._raw_udt  # TODO: Decide if this could instead be tracked as integer of first bin utime
        # Can this rolling operation be done with np.add.at(data[1:]-data[:-1], arange(1, data.size-1)?
        self._data.advance()

    def update_buffer(self, idx_dom):
        """Add current payload to staging buffer according to DOM index

        Parameters
        ----------
        idx_dom : int
            Index of staging buffer at which to add the data contained by the current payload

        Notes
        -----
        It is assumed that idx_dom corresponds to the current payload contained by _pay
        """
        data, idx_data = c_rebin_scalers(self._raw_utime[0], self._pay.utime, self._pay.scaler_bytes)
        if data.size > 0:
            self._data.add(data, idx_dom, idx_data)
        self._payloads_read[idx_dom] += 1

    def rebin_scalers(self, utime, scaler_bytes):
        """Rebin scalers to 2 ms

        Parameters
        ----------
        utime : int
            Time payload in 0.1 ns since start of year
        scaler_bytes : bytearray
            Binary scaler hit data

        Returns
        -------
        counts : np.ndarray
            Scalers restructured in 2 ms bins
        idx : np.ndarray
            Indices in which the new scaler counts should be added

        Notes
        -----
        Could be changed to increment bin time as np.uint16 rather than thru array elements
        """
        scalers = np.frombuffer(scaler_bytes, dtype=np.uint8)
        idx_sclr = scalers.nonzero()[0]
        if idx_sclr.size == 0:
            return np.array([]), np.array([])

        raw_counts = np.zeros(self._staging_depth, dtype=np.uint8)
        scaler_utime = utime + idx_sclr*self._scaler_udt
        idx_raw = self._raw_utime.searchsorted(scaler_utime, side="left") - 1
        np.add.at(raw_counts, idx_raw, scalers[idx_sclr])
        # Duplicate entries in idx_base (when two 1.6ms bins have bin starts in same 2ms bin) must be added
        # like so, not via base_counts[idx_base] += scalers[idx_sclr] which only performs addition for first idx
        # idx_base

        cut = (scaler_utime + self._scaler_udt > self._raw_utime[idx_raw] + self._raw_udt) & \
              (scaler_utime < self._raw_utime[idx_raw] + self._raw_udt)
        idx_raw = idx_raw[cut]
        idx_sclr = idx_sclr[cut]

        frac = 1. - ((self._raw_utime[idx_raw] + self._raw_udt - scaler_utime[cut])/self._scaler_udt)
        raw_counts[idx_raw] -= np.uint8(0.5+frac*scalers[idx_sclr])
        raw_counts[idx_raw+1] += np.uint8(0.5+frac*scalers[idx_sclr])

        # Passing arrays like this may increase overhead and reduce efficiency
        idx_raw = raw_counts.nonzero()[0]
        return raw_counts[idx_raw], idx_raw

    @property
    def raw_dt(self):
        """Scaler bin size after rebinning, in ms

        Returns
        -------
            Scaler bin size, in units ms, after being processed by rebin_scalers

        See Also
        --------
        sndaq.datahandler.rebin_scalers
        """
        """Return binsize in ms for scalers after rebinning, Default = 2 ms"""
        return self._raw_dt

    @property
    def raw_udt(self):
        """Scaler bin size after rebinning, in 0.1 ns

        Returns
        -------
            Scaler bin size, in units 0.1 ns, after being processed by rebin_scalers

        See Also
        --------
        sndaq.datahandler.rebin_scalers
        """
        return self._raw_udt

    @property
    def scaler_dt(self):
        """Scaler bin size before rebinning, in ms

        Returns
        -------
        binsize : float
            Scaler bin size, in units ms, from file. Expected = 1.6384 ms"""
        return self.scaler_dt

    @property
    def scaler_udt(self):
        """Scaler bin size before rebinning, in 0.1 ns

        Returns
        -------
        binsize : int
            Scaler bin size in units 0.1 ns from file
        """
        return self._scaler_udt

    @staticmethod  # TODO Change this away from static method when using config
    def get_cand_rmu(cand):
        """Update candidate with R_mu around trigger time

        Parameters
        ----------
        cand : sndaq.trigger.Candidate
        """
        t0 = None
        rebin_factor = cand.trigger.binsize // 500
        trigger_utime = (cand.trigger.t - cand.trigger.t.astype('datetime64[Y]')).astype('timedelta64[ns]').astype(int)
        udt = int(500e6)  # 500 ms, in ns

        # TODO: Make this bases this on config or duration of files. currently this assumes ~60 s per rate file
        nbins = 2 * 60 * len(cand.rmu_files+1)
        rmu_base = np.zeros(nbins)
        utime_binned = np.arange(nbins) * udt

        for file in cand.rmu_files:
            # TODO: Change this to reference class member (IE self.pdaqtrigger_reader) for configured data sources
            with PDAQ_PayloadReader(file) as rdr:
                rmu = rdr.read_payloads()
            utime = (rmu['t'] - rmu['t'].astype('datetime64[Y]')).astype(int)
            if not t0:
                # This ensures bin edges align with trigger bin edge
                t0 = trigger_utime - udt * (1 + (trigger_utime - utime[0])//udt)

            idx = utime_binned.searchsorted(utime - t0)
            np.add.at(rmu_base, idx, rmu['rmu'])

        # TODO: Make this based on config, rather than hardcoded 500 (ms)
        idx = np.arange(nbins).reshape(-1, 1) + np.arange(rebin_factor)
        # Obtain muon rate estimation in trigger binsize by summing over every `rebin_factor`-sized slice of bins
        rmu_trigger = np.append(rmu_base, np.zeros(rebin_factor - 1))[idx].sum(axis=1)
        cand.rmu_base = rmu_base / (rebin_factor * 0.5)  # Report in units Hz
        cand.rmu_trigger = rmu_trigger / 0.5  # Report in units Hz


