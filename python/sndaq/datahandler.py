import numpy as np
import glob
from sndaq.reader import SN_PayloadReader


class DataHandler:

    def __init__(self, ndom=5160, dtype=np.uint16):

        self._scaler_udt = int(250 * 2**16)
        self._scaler_dt = self.scaler_udt / 1e7
        self._raw_dt = 2
        self._raw_udt = int(self._raw_dt * 1e7)
        self._staging_depth = 2000
        self._payloads_read = np.zeros(5160, dtype=np.uint16)

        self._data = np.zeros((ndom, self._staging_depth), dtype=dtype)
        self._raw_utime = np.zeros(self._staging_depth, dtype=np.uint32)

        self._file = None
        self._file_glob = None
        self._pay = None
        self._start_utime = None
        self._file_start_utime = None

    @property
    def files(self):
        return self._file_glob()

    def get_data_files(self, directory):
        self._file_glob = sorted(glob.glob('/'.join((directory, 'sn*.dat'))))  # May need to check sorting order

    def set_file(self, filename):
        self._file = SN_PayloadReader(filename)

    def read_payload(self):
        self._pay = next(self._file)

    @property
    def payload(self):
        return self._pay

    @property
    def next_scalers(self):  # TODO: Figure out a better name
        """Return rebinned scalers at the tail of the staging buffer"""
        return self._data[:, 0]

    def advance_buffer(self):
        self._raw_utime += self._raw_udt
        # Can this rolling operation be done with np.add.at(data[1:]-data[:-1], arange(1, data.size-1)?
        self._data = np.roll(self._data, -1, axis=1)
        self._data[:, -1] = 0

    def update_buffer(self, idx_dom):
        data, idx_data = self.rebin_scalers(self._pay.utime, self._pay.scaler_bytes)
        np.add.at(self._data, (idx_dom, idx_data), data)
        self._payloads_read[idx_dom] += 1

    def rebin_scalers(self, utime, scaler_bytes):
        """time_bins is base (2ms) time bins
            Could be changed to increment bin time as np.uint16 rather than thru array elements
        """
        scalers = np.frombuffer(scaler_bytes, dtype=np.uint8)
        idx_sclr = scalers
        raw_counts = np.zeros(self._staging_depth, dtype=np.uint8)
        if idx_sclr.size == 0:
            return raw_counts

        scaler_utime = utime + idx_sclr*self._scaler_udt
        idx_raw = self._raw_utime.searchsorted(scaler_utime, side="left") - 1
        np.add.at(raw_counts, idx_raw, scalers[idx_sclr])
        # Duplicate entries in idx_base (when two 1.6ms bins have bin starts in same 2ms bin) must be added
        # like so, not via base_counts[idx_base] += scalers[idx_sclr] which only performs addition for first idx
        # idx_base

        cut = (scaler_utime + self._scaler_udt + self._raw_udt > self._raw_utime[idx_raw]) & \
              (scaler_utime < self._raw_utime[idx_raw] + self._raw_udt)
        idx_raw = idx_raw[cut]
        idx_sclr = idx_sclr[cut]

        frac = 1. - ((self._raw_utime[idx_raw] + self._raw_udt - scaler_utime[cut])/self._scaler_udt)
        raw_counts[idx_raw] -= np.uint16(0.5+frac*scalers[idx_sclr])
        raw_counts[idx_raw+1] += np.uint16(0.5+frac*scalers[idx_sclr])

        # Passing arrays like this may increase overhead and reduce efficiency
        idx_raw = raw_counts.nonzero()[0]
        return raw_counts[idx_raw], idx_raw

    @property
    def raw_dt(self):
        """Return binsize in ms for scalers after rebinning, Default = 2 ms"""
        return self._raw_dt

    @property
    def raw_udt(self):
        """Return binsize in 0.1 ns (units utime) for scalers after rebinning"""
        return self._raw_udt

    @property
    def scaler_dt(self):
        """Return binsize in ms for scalers from sndata file, Default = 1.6384 ms"""
        return self.scaler_dt

    @property
    def scaler_udt(self):
        """Return binsize in ms for scalers after rebinning"""
        return self._scaler_udt
