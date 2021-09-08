"""Writer objects for SN scaler data
"""
import os
import gzip
import bz2
import struct
from sndaq.reader import SN_Payload, SN_MAGIC_NUMBER


class Writer(object):
    """Construct and write payloads to file"""

    def __init__(self, filename: str, overwrite: bool = False):
        """Open a payload file

        :param filename: Name of payload file
        :type filename: str
        """
        if not os.path.exists(filename) and not overwrite:
            raise Exception("File \"{0:s}\" exists. Use kwarg \'overwite=True\' to force overwrite".format(filename))

        if filename.endswith(".gz"):
            fout = gzip.open(filename, "wb")
        elif filename.endswith(".bz2"):
            fout = bz2.BZ2File(filename)
        elif filename.endswith(".dat"):
            fout = open(filename, "wb")
        else:
            fout = open(filename, "wb")

        self._filename = filename
        self._fout = fout
        self._num_written = 0

    def __enter__(self):
        """Return this object as a context manager to used as `with SN_PayloadWriter(filename) as wrtr:`
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Close the open filehandle when the context manager exits
        """
        self.close()

    def close(self):
        """Explicitly close the filehandle

        :return: None
        :rtype: None
        """
        if self._fout is not None:
            try:
                self._fout.close()
            finally:
                self._fout = None

    @property
    def nrec(self) -> int:
        """Number of payloads written to this point

        :return: self.__num_written
        :rtype: int
        """
        return self._num_written

    @property
    def filename(self) -> str:
        """Name of file being written

        :return: self.__filename
        :rtype: str
        """
        return self._filename


def construct_payload(utime, dom_id, domclock, scalers, keep_data=True):
    """Create a new SN_Payload

    Parameters
    ----------
    utime : int (uint32)
        UTC timestamp from year start
    dom_id : int (uint32)
        DOM Motherboard ID
    domclock : int
        Number of DOM clock cycles elapsed since DOM launch
    scalers : array_like
        SN scaler values
    keep_data : bool
        If True, construct a payload with all fields
        If False, construct a payload header only

    Returns
    -------
    payload : sndaq.reader.SN_Payload
        Custom SN scaler payload

    See Also
    --------
    sndaq.reader.SN_Payload

    """
    rawdata = struct.pack('>QHH6B{0:d}B'.format(len(scalers)), dom_id, len(scalers) + 10, SN_MAGIC_NUMBER,
                          *domclock.to_bytes(length=6, byteorder='big'), *scalers)
    # > - Big endian
    # Q  - integer (8 bytes) - DOM mainboard ID, flds[0]
    # H  - integer (2 bytes) - record length in bytes (10+scaler_len), flds[1]
    # H  - integer (2 bytes) - SN record MAGIC NUMBER, flds[2]
    # 6B - 6 integers (1 byte each) - DOM Clock bytes, flds[3:9]
    # {0:d}B - scaler_len integers (1 byte each) - scaler data, flds[9:]
    return SN_Payload(utime, rawdata, keep_data)


class SN_PayloadWriter(Writer):
    """Write SN scaler payloads from a file
    """
    def __init__(self, filename: str, overwrite=False):
        super().__init__(filename, overwrite)

    def write(self, payload):
        """Write a SN scaler payload to file

        Parameters
        ----------
        payload : sndaq.payload.SN_Payload
        """
        self._fout.write(payload.record_bytes)
