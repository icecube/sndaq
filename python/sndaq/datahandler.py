"""Objects and functions for handling and processing raw data used by SNDAQ
"""
import numpy as np
import glob
from sndaq.reader import SN_PayloadReader
from sndaq.buffer import stagingbuffer


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

        self._file = None
        self._file_glob = None
        self._pay = None
        self._start_utime = None
        self._file_start_utime = None

    @property
    def files(self):
        """List of SN scaler file paths

        Returns
        -------
        files : list
            Paths to files containing unprocessed sn scaler data
        """
        return self._file_glob

    def get_data_files(self, directory):
        """Get SN scaler files from a directory

        Parameters
        ----------
        directory : str | os.PathLike
            Directory to search for SN data files
        """
        self._file_glob = sorted(glob.glob('/'.join((directory, 'sn*.dat'))))  # May need to check sorting order

    def set_file(self, filename):
        """Set current SN scaler data file

        Parameters
        ----------
        filename : str | os.PathLike
            String or path of SN Scaler data file
        """
        self._file = SN_PayloadReader(filename)

    def read_payload(self):
        """Read one SN scaler payload from the current file

        See Also
        --------
        sndaq.reader.SN_PayloadReader
        sndaq.reader.SN_Payload

        """
        self._pay = next(self._file)

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
        self._raw_utime += self._raw_udt
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
        data, idx_data = self.rebin_scalers(self._pay.utime, self._pay.scaler_bytes)
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

