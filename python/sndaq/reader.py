"""Reader objects for SN scaler data and pdaq muon data
"""

import bz2
import gzip
import numbers
import os
import struct
from datetime import datetime
from sndaq.datetime_ns import datetime_ns

# Type annotations
from typing import Tuple, Union, BinaryIO, TextIO, Type, Optional
from types import TracebackType

SN_TYPE_ID = 16
SN_ENVELOPE_LENGTH = 16
SN_HEADER_LENGTH = 18
SN_MAGIC_NUMBER = 300


class Reader(object):
    """Reader object for SN data
    """
    def __init__(self, filename, filetype, keep_data=True):
        """Open a payload file

        :param filename: Name of payload file
        :type filename: str
        :param keep_data: If true, write scaler data to SN_Payload, if false scaler data will be written as None
        :type keep_data: bool
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

    def __exit__(self, exc_type: Optional[Type[Exception]],
                 exc_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        """Close the open filehandle when the context manager exits
        """
        self.close()

    def __iter__(self):
        """Generator which returns payloads in `for payload in payrdr:` loops

        :return: Payload or None if EOF reached
        :rtype: SN_Payload or None
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

        :return: None
        :rtype: None
        """
        if self._fin is not None:
            try:
                self._fin.close()
            finally:
                self._fin = None

    @property
    def nrec(self) -> int:
        """Number of payloads read to this point

        :return: self.__num_read
        :rtype: int
        """
        return self._num_read

    @property
    def filename(self) -> str:
        """Name of file being read

        :return: self.__filename
        :rtype: str
        """
        return self._filename


class SN_Payload(object):
    """Reader object for SN scaler payloads
    """
    def __init__(self, utime: int, data: bytes, keep_data: bool = True) -> None:
        """Convert SN scaler record data bytes into payload object.
        See PayloadReader.decode_payload() for full description of record format.
        Assumes argument data contains 18 bytes of additional payload fields before scaler data begins.

        :param utime: UTC timestamp from year start
        :type utime: int

        :param data: SN record bytes
        :type data: bytearray

        :param keep_data: Switch to keep data (true) or skip (false)
        :type keep_data: bool
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
    def extract_clock_bytes(clock_bytes: Union[numbers.Number, list, tuple]) -> Tuple[int]:
        """

        Parameters
        ----------
        clock_bytes :

        Returns
        -------

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
    def dom_id(self) -> int:
        """

        Returns
        -------

        """
        return self.__dom_id

    @property
    def domclock(self) -> int:
        """Number of DOM Clock cycles

        :return: number of DOM clock cycles
        :rtype: int
        """
        val = 0
        for byte in self.__clock_bytes:
            val = (val << 8) + byte
        return val

    @property
    def record_bytes(self) -> bytes:
        """Binary representation of record, will only contain scaler data if keep_data = True

        :return: binary representation of record
        :rtype: bytes
        """
        if not self.has_data:
            return self.envelope
        else:
            return self.envelope + self.__data

    @property
    def envelope(self) -> bytes:
        """Binary representation of record envelope

        :return: binary representation of envelope
        :rtype: bytes
        """
        return struct.pack(">2IQ", self.data_length + SN_ENVELOPE_LENGTH, self.type_id, self.__utime)

    @property
    def data_bytes(self) -> Union[None, bytes]:
        """Binary representation of record data (all fields other than envelope)

        :return: binary representation of record data
        :rtype: bytearray
        """
        if not self.has_data:
            return None
        else:
            return self.__data

    @property
    def data_length(self) -> int:
        """Number of data bytes (number of scaler bytes + 18)

        :return: number of data bytes
        :rtype: int
        """
        if not self.has_data:
            return 0
        else:
            return len(self.__data)

    @property
    def scaler_bytes(self) -> Union[None, bytes]:
        """Binary representation of scaler bytes

        :return: binary representation of scaler data
        :rtype: bytearray
        """
        if not self.has_data:
            return None
        else:
            return self.__data[SN_HEADER_LENGTH:]

    @property
    def scaler_length(self) -> int:
        """Number of scaler bytes

        :return: number of scaler bytes
        :rtype: int
        """
        if not self.has_data:
            return 0
        else:
            return self.data_length - SN_HEADER_LENGTH

    @property
    def has_data(self) -> bool:
        """Indicates if SN Payload contains SN scaler data (true) or not (false)

        :return: indicates if SN payload contains SN scaler data
        :rtype: bool
        """
        return self.__has_data

    @property
    def type_id(self) -> int:
        """IceCube Payload ID (Always 16)

        :return: IceCube Supernova Payload ID
        :rtype: int
        """
        return SN_TYPE_ID

    @property
    def source_name(self) -> str:
        """Name of IceCube Payload Source

        :return: IceCube payload Source
        :rtype: str
        """
        return "Supernova Payload"

    @property
    def utime(self) -> int:
        """UTC Timestamp of record since start of year in 0.1ns

        :return: UTC Timestamp of record since start of year in 0.1ns
        :rtype: int
        """
        return self.__utime


class SN_PayloadReader(Reader):
    """Read DAQ payloads from a file"""

    def __init__(self, filename: str, keep_data: bool = True) -> None:
        super().__init__(filename, filetype='sndata', keep_data=keep_data)

    def __next__(self) -> SN_Payload:
        """Read the next payload

        :return: pay
        :rtype: SN_Payload
        """
        pay = self.decode_payload(self._fin, keep_data=self._keep_data)
        self._num_read += 1
        return pay

    @classmethod
    def decode_payload(cls, stream: BinaryIO, keep_data: bool = True) -> Union[None, SN_Payload]:
        """Decode and return the next payload.

        :param stream: File object containing bytes open in read mode
        :type stream: file
        :param keep_data: If true, write scaler data to SN_Payload, if false scaler data will be written as None
        :type keep_data: bool
        :return: Supernova Payload
        :rtype: SN_Payload
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

    def __init__(self, filename: str, keep_data: bool = True) -> None:
        super().__init__(filename, filetype='pdaqtrigger', keep_data=keep_data)

    def __next__(self) -> Union[Tuple[datetime_ns, int], datetime_ns]:
        """Read the next payload

        :return: payload
        :rtype: PDAQ_Payload
        """
        payload = self.decode_payload(self._fin)
        self._num_read += 1
        return payload

    @classmethod
    def decode_payload(cls, stream: TextIO, keep_data: bool = True) -> Union[Tuple[datetime_ns, int], datetime_ns]:
        """Decode and return the next payload.

        :param stream: File object containing bytes open in read mode
        :type stream: TextIO
        :param keep_data: If true, write scaler data to SN_Payload, if false scaler data will be written as None
        :type keep_data: bool
        :return: pDAQ Trigger Payload
        :rtype: datetime_ns, Tuple
        """
        raw_data = stream.readline().split()
        year, month, day = [int(x) for x in raw_data[0].split('-')]
        hour, minute, sec = [int(float(x)) for x in raw_data[1].split(':')]
        ns = int((float(raw_data[1].split(':')[2]) - sec) * 1e10) / 10
        trigger_count = int(raw_data[2])
        # return (PDAQ_Payload...)
        if keep_data:  # This probably isn't necessary
            return datetime_ns(datetime(year, month, day, hour, minute, sec), ns), trigger_count
        else:
            return datetime_ns(datetime(year, month, day, hour, minute, sec), ns)


def read_file(filename, max_payloads) -> None:
    """

    Parameters
    ----------
    filename :
    max_payloads :
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
