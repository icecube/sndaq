"""Reader objects for SN scaler data and pdaq muon data
"""

import bz2
import gzip
import numbers
import os
import struct
from datetime import datetime
from sndaq.datetime_ns import datetime_ns

SN_TYPE_ID = 16
SN_ENVELOPE_LENGTH = 16
SN_HEADER_LENGTH = 18
SN_MAGIC_NUMBER = 300


class Reader(object):
    """Reader object for SN data
    """
    def __init__(self, filename, filetype, keep_data=True):
        """Open a payload file

        Parameters
        ----------
        filename : str
            Name of payload file
        filetype : str
            Type of payload file used to determine file open mode in conjunction with file extension
        keep_data : bool
            If True, construct payloads with all fields
            If False, construct payloads with headers only
        """
        if not os.path.exists(filename):
            raise Exception("Cannot read \"{0:s}\"".format(filename))

        if filename.endswith(".gz"):
            fin = gzip.open(filename, "rb")
        elif filename.endswith(".bz2"):
            fin = bz2.BZ2File(filename)
        elif filename.endswith(".dat") and filetype is 'sndata':
            fin = open(filename, "rb")
        elif filename.endswith(".dat") and filetype is 'pdaqtrigger':
            fin = open(filename, "r")
        else:
            fin = open(filename, "rb")

        self._filename = filename
        self._fin = fin
        self._keep_data = keep_data
        self._num_read = 0

    def __enter__(self):
        """Return this object as a context manager to used as `with SN_PayloadReader(filename) as rdr:`
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Close the open filehandle when the context manager exits
        """
        self.close()

    def __iter__(self):
        """Iterate through payloads of current file

        Returns
        -------
        Payload : SN_Payload | None
            Payload read from file, or None if EOF has been reached.
        """
        while True:
            if self._fin is None:
                # generator has been explicitly closed
                return

            # decode the next payload
            pay = next(self)
            if pay is None:
                # must have hit the end of the file
                return

            # return the next payload
            yield pay

    def __next__(self) -> ...:
        pass

    def close(self):
        """Explicitly close the filehandle

        Returns
        -------
        None : None
        """
        if self._fin is not None:
            try:
                self._fin.close()
            finally:
                self._fin = None

    @property
    def nrec(self):
        """Number of payloads read to this point

        Returns
        -------
        num_read : int
            Number of payloads that have been read from the current file
        """
        return self._num_read

    @property
    def filename(self):
        """Name of current file

        Returns
        -------
        filename : str
            Name of current file
        """
        return self._filename


class SN_Payload(object):
    """Reader object for SN scaler payloads
    """
    def __init__(self, utime, data, keep_data=True):
        """Convert SN scaler record data bytes into payload object.
        Assumes argument data contains 18 bytes of additional payload fields before scaler data begins.

        Parameters
        ----------
        utime : int
            Time of payload since start of year in units 0.1 ns
        data : bytearray | bytes
            Binary representation of payload data other than header information
        keep_data : bool
            If True, construct a payload with all fields
            If False, construct a payload header only
        """
        self.__utime = utime

        if keep_data and data is not None:  # Keep data, data must be defined
            self.__data = data
            self.__has_data = True
        else:
            self.__data = None
            self.__has_data = False

        if self.__has_data:
            # Read SN Payload fields from bytes.
            scaler_len = len(data) - 18
            flds = struct.unpack(">QHH6B{0:d}B".format(scaler_len), data)
            # > - Big endian
            # Q  - integer (8 bytes) - DOM mainboard ID, flds[0]
            # H  - integer (2 bytes) - record length in bytes (10+scaler_len), flds[1]
            # H  - integer (2 bytes) - SN record MAGIC NUMBER, flds[2]
            # 6B - 6 integers (1 byte each) - DOM Clock bytes, flds[3:9]
            # {0:d}B - scaler_len integers (1 byte each) - scaler data, flds[9:]

            self.__dom_id = flds[0]
            self.__clock_bytes = flds[3:9]
            self.__scaler_bytes = flds[9:]

    @staticmethod  # Decorator allows this method to be called without initializing the object
    def extract_clock_bytes(clock_bytes):
        """Read 6-byte clock field

        Parameters
        ----------
        clock_bytes : array_like
            6 bytes used to construct the number of clock cycles since DOM activation

        Returns
        -------
        domclock : int
            Number of 25 ns clock cycles since DOM activation
        """
        if isinstance(clock_bytes, numbers.Number):
            tmpbytes = []
            for _ in range(6):
                tmpbytes.insert(0, int(clock_bytes & 0xff))
                clock_bytes >>= 8
            return tuple(tmpbytes)

        if isinstance(clock_bytes, list) or isinstance(clock_bytes, tuple):
            if len(clock_bytes) != 6:
                raise Exception(f"Expected 6 clock bytes, not {len(clock_bytes):d}")
            return clock_bytes

        raise Exception(f"Cannot convert {type(clock_bytes).__name__}")

    def __str__(self) -> str:
        return "Supernova@{0:d}[dom {1:012x} clk {2:012x} scalerData*{3:d}".format(
            self.utime, self.__dom_id, self.domclock, len(self.__scaler_bytes)
        )

    @property
    def dom_id(self):
        """DOM mainboard ID

        Returns
        -------
        dom_id : int
            DOM mainboard ID as base 10 integer
        """
        return self.__dom_id

    @property
    def domclock(self):
        """Number of DOM Clock cycles

        Returns
        -------
        domclock : int
            Number of 25 ns clock cycles since DOM activation
        """
        val = 0
        for byte in self.__clock_bytes:
            val = (val << 8) + byte
        return val

    @property
    def record_bytes(self):
        """Binary representation of payload

        Returns
        -------
        record_bytes : bytearray
            Binary representation of payload, will only contain scaler data if keep_data = True
        """
        if not self.has_data:
            return self.envelope
        else:
            return self.envelope + self.__data

    @property
    def envelope(self) -> bytes:
        """Binary representation of payload envelope

        Returns
        -------
        envelope_bytes : bytearray
             binary representation of payload envelope, containing only the payload size, type ID and utime
        """
        return struct.pack(">2IQ", self.data_length + SN_ENVELOPE_LENGTH, self.type_id, self.__utime)

    @property
    def data_bytes(self):
        """Binary representation of payload data

        Returns
        -------
        data_bytes : bytearray | None
            Binary representation of payload data fields, all other than payload size, type ID and utime
            If this payload was constructed with keep_data=False, this will return None
        """
        if not self.has_data:
            return None
        else:
            return self.__data

    @property
    def data_length(self):
        """Number of data bytes

        Returns
        -------
        data_length : int
            Number of data bytes in payload, this equals number of scaler bytes + 18
        If this payload was constructed with keep_data=False, this will return 0
        """
        if not self.has_data:
            return 0
        else:
            return len(self.__data)

    @property
    def scaler_bytes(self):
        """Number of scaler bytes

        Returns
        -------
        scaler_bytes : bytearray | None
            Binary representation of payload scalers
            If this payload was constructed with keep_data=False, this will return None
         """
        if not self.has_data:
            return None
        else:
            return self.__data[SN_HEADER_LENGTH:]

    @property
    def scaler_length(self):
        """Number of data bytes

        Returns
        -------
        scaler_length : int
            Number of scaler bytes in payload
            If this payload was constructed with keep_data=False, this will return 0
        """
        if not self.has_data:
            return 0
        else:
            return self.data_length - SN_HEADER_LENGTH

    @property
    def has_data(self):
        """Indicates if payload has Data

        Returns
        -------
        has_data :
            If True, payload was constructed with keep_data = True, and has data bytes
            If False, payload was constructed with keep_data = False, and has only the envelope
        """
        return self.__has_data

    @property
    def type_id(self):
        """IceCube Payload ID

        Returns
        -------
        type_id : int
            IceCube Supernova scaler payload ID, should always be 16
        """
        return SN_TYPE_ID

    @property
    def source_name(self):
        """IceCube Payload Source

        Returns
        -------
        source_id : str
            Named IceCube payload Source, always "Supernova payload"
        """
        return "Supernova Payload"

    @property
    def utime(self):
        """Time of payload, in 0.1 ns

        Returns
        -------
        utime : int
            UTC Timestamp of payload in units 0.1 ns since start of year
        """
        return self.__utime


class SN_PayloadReader(Reader):
    """Read DAQ payloads from a file"""

    def __init__(self, filename, keep_data=True):
        """Open a SN scaler payload file

        Parameters
        ----------
        filename : str
            Name of SN scaler payload file
        keep_data : bool
            If True, construct payloads with all fields
            If False, construct payloads with headers only

        Examples
        --------
        >>> with SN_PayloadReader(...) as rdr:
        >>>     for payload in rdr:
        >>>         print(payload)
        >>>         break
        Supernova@278941064807639342[dom 9486d3ddbece clk 000000000000 scalerData*602]
        """
        super().__init__(filename, filetype='sndata', keep_data=keep_data)

    def __next__(self):
        """Read the next payload

        Returns
        -------
        payload : sndaq.reader.SN_Payload
        """
        pay = self.decode_payload(self._fin, keep_data=self._keep_data)
        self._num_read += 1
        return pay

    @classmethod
    def decode_payload(cls, stream, keep_data=True):
        """Decode and return the next payload

        Parameters
        ----------
        stream : BinaryIO
            File object containing bytes open in read binary mode
        keep_data : Bool
            If true, construct payload with all fields
            If false, construct a payload with header only.

        Returns
        -------
        Payload : sndaq.reader.SN_Payload
            SN scaler payload
        """
        envelope = stream.read(SN_ENVELOPE_LENGTH)
        if len(envelope) == 0:
            return None

        # Read in payload envelope
        length, type_id, utime = struct.unpack(">iiq", envelope)
        # > - big endian
        # i - int (4 bytes) - payload length in bytes
        # i - int (4 bytes) - payload type, should be 16
        # q - long long (8 bytes) - UTC timestamp
        if length <= SN_ENVELOPE_LENGTH:
            rawdata = None
        else:
            rawdata = stream.read(length - SN_ENVELOPE_LENGTH)

        return SN_Payload(utime, rawdata, keep_data=keep_data)


class PDAQ_PayloadReader(Reader):
    """Reader for PDAQ SMT8 trigger rate data"""

    def __init__(self, filename, keep_data=True):
        """Open a PDAQ trigger rate data file

        Parameters
        ----------
        filename : str
            Name of PDAQ trigger rate file
        keep_data :
            If True, include trigger rate
            If False, include timestamp only

        Examples
        --------
        >>> with PDAQ_PayloadReader(...) as rdr:
        >>>     for payload in rdr:
        >>>         print(payload)
        >>>         break
        (2021 - 03 - 25 19:41:47.3729524339, 12)
        """
        super().__init__(filename, filetype='pdaqtrigger', keep_data=keep_data)

    def __next__(self):
        """Read the next payload

        Returns
        --------
        payload : sndaq.reader.PDAQ_Payload

        :return: payload
        :rtype: PDAQ_Payload
        """
        payload = self.decode_payload(self._fin)
        self._num_read += 1
        return payload

    @classmethod
    def decode_payload(cls, stream, keep_data=True):
        """Decode and return the next payload

        Parameters
        ----------
        stream : file
            File object containing PDAQ Trigger payloads open in read mode
        keep_data :
            If True, include trigger rate
            If False, include timestamp only

        Returns
        -------
        Payload : tuple, datetime_ns
            If keep_data = True, returns tuple of trigger time and trigger rate
            If keep_data = False, returns trigger timestamp only

        """


def read_file(filename, max_payloads):
    """Read a fixed number of payloads from a file, and print them to std out

    Parameters
    ----------
    filename : str
        Path to file containing payloads
    max_payloads : int
        Number of payloads to read
    """
    with PDAQ_PayloadReader(filename) as rdr:
        for pay in rdr:
            if max_payloads is not None and rdr.nrec > max_payloads:
                break

            print(str(pay))


if __name__ == "__main__":
    # read_file('./scratch/data/sn-0.dat', 10)
    read_file('./scratch/data/pDaqTriggers_7363.dat', 10)

    # SN scaler data file is located at /home/sgriswold/sn-0.dat (04/10/2021)
    # Note, as written read_file will always start at the beginning of the file when called
    # Expected printed output as follows:
    # Supernova@278941064807639342[dom 9486d3ddbece clk 000000000000 scalerData*602
    # Supernova@278941064807846976[dom 53f98da44b15 clk 000000000000 scalerData*602
    # Supernova@278941064808092163[dom 1fa7b4422fb3 clk 000000000000 scalerData*602
    # Supernova@278941064808274711[dom 874f6acd472e clk 000000000000 scalerData*602
    # Supernova@278941064808810516[dom c54ba499f024 clk 000000000000 scalerData*602
    # Supernova@278941064810075365[dom 58191b7511ea clk 000000000000 scalerData*602
    # Supernova@278941064810262845[dom 7e15a8e98ef4 clk 000000000000 scalerData*602
    # Supernova@278941064810445403[dom 1d4b599df4b8 clk 000000000000 scalerData*602
    # Supernova@278941064810867258[dom 86e958e3f2a0 clk 000000000000 scalerData*602
    # Supernova@278941064811162841[dom b6735e6efb7c clk 000000000000 scalerData*602
    # Supernova@278941064810867258[dom 86e958e3f2a0 clk 000000000000 scalerData*602

    # PDAQ triggger data file is located at /home/sgriswold/
    # Note, as written read_file will always start at the beginning of the file when called
    # Expected printed output as follows
    # (2021 - 03 - 25 19:41:47.3729524339, 12)
    # (2021 - 03 - 25 19:41:47.3729796776, 9)
    # (2021 - 03 - 25 19:41:47.3738086247, 38)
    # (2021 - 03 - 25 19:41:47.3738211212, 10)
    # (2021 - 03 - 25 19:41:47.3742992067, 14)
    # (2021 - 03 - 25 19:41:47.3754195878, 11)
    # (2021 - 03 - 25 19:41:47.3760853555, 15)
    # (2021 - 03 - 25 19:41:47.3781348106, 24)
    # (2021 - 03 - 25 19:41:47.3790544452, 37)
    # (2021 - 03 - 25 19:41:47.3791499375, 76)
