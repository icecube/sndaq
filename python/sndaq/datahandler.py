"""Objects and functions for handling and processing raw data used by SNDAQ
"""
import numpy as np
import glob
import os
import requests
import json
from time import sleep

from sndaq.reader import SN_PayloadReader, PDAQ_PayloadReader
from sndaq.buffer import stagingbuffer
from sndaq.util.rebin import rebin_scalers as c_rebin_scalers


class DataHandler:
    """Handler for SN scaler data files
    """
    def __init__(self, ndom=5160, dtype=np.uint16):
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
        self._staging_depth = 2000
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

    def get_run_no(self):
        # TODO: IMPORTANT!!! DO NOT HARD CODE THIS
        return 260299

    def get_scaler_files(self, directory, start_time=None, stop_time=None, buffer_time_l=None, buffer_time_t=None):
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
        """
        self._scaler_file_glob = sorted(glob.glob('/'.join((directory, 'sn*.dat'))))  # May need to check sorting order

        if start_time:
            start_datetime = np.datetime64(start_time)
            if stop_time:
                stop_datetime = np.datetime64(stop_time)
            else:
                stop_datetime = start_datetime

            # Estimate scaler file times
            idc = np.array([os.path.basename(file).split('_')[2] for file in self._scaler_file_glob])

            # TODO Figure out a better way of getting run start time
            run_no = self.get_run_no()
            DATA = {
                'user': 'REDACTED',
                'pass': 'REDACTED',
                'run_number': run_no
            }
            sleep(1)  # Required to prevent accidental DDoS
            # TODO: Add this to config
            response = requests.post(url="https://virgo.icecube.wisc.edu/run_info/{run_no}", data=DATA)
            data = json.loads(response.text)
            run_start = np.datetime64(data['run_start'])
            file_times = np.datetime64(60, 's') * idc + run_start  # Each file is about a minute
            t0 = start_datetime - np.timedelta64(buffer_time_t, 'ms')  # Add a minute to ensure
            t1 = stop_datetime + np.timedelta64(buffer_time_l, 'ms')  # Add a minute to ensure files
            self._scaler_file_glob = np.where(self._scaler_file_glob, (t0 < file_times) & (file_times < t1))[0]

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


