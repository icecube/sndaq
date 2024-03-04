import bz2
import gzip
import os
import struct

HS_TYPE_ID = 3
HS_ENVELOPE_LENGTH = 32
HS_HEADER_LENGTH = 22


class Reader(object):
    """Read payloads from a file"""

    def __init__(self, filename: str, filetype: str, keep_data: bool = True) -> None:
        """Open a payload file

        :param filename: Name of payload file
        :type filename: str
        :param keep_data: If true, write waveform data to HS_payload, if false waveform data will be written as None
        :type keep_data: bool
        """
        if not os.path.exists(filename):
            raise Exception("Cannot read \"{0:s}\"".format(filename))

        if filename.endswith(".gz"):
            fin = gzip.open(filename, "rb")
        elif filename.endswith(".bz2"):
            fin = bz2.BZ2File(filename)
        elif filename.endswith(".dat") and filetype in ['sndata', 'hitspool']:
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
        """Return this object as a context manager to used as `with PayloadReader(filename) as payrdr:`
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the open filehandle when the context manager exits
        """
        self.close()

    def __iter__(self):
        """Generator which returns payloads in `for payload in payrdr:` loops

        :return: Payload or None if EOF reached
        :rtype: HS_payload or None
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

    def __next__(self):
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
    def nrec(self):
        """Number of payloads read to this point

        :return: self.__num_read
        :rtype: int
        """
        return self._num_read

    @property
    def filename(self):
        """Name of file being read

        :return: self.__filename
        :rtype: str
        """
        return self._filename


class HS_Payload(object):
    """Data Container for HitSpool Delta Compressed Hits
    """

    def __init__(self, utime, dom_id, data, keep_data=True):
        """Convert HitSpool record data bytes into payload object.

        See PayloadReader.decode_payload() for full description of record format.
        Assumes argument data contains 38 bytes of additional payload fields before waveform data begins.

        Parameters
        ----------
        utime : int
            UTC timestamp from year start
        dom_id : int
            DOM Motherboard ID number
        data : bytearray or bytes
            compressed waveform bytes
        keep_data : bool
            Switch to keep data (true) or skip (false)
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
            waveform_len = len(data) - HS_HEADER_LENGTH
            flds = struct.unpack(">3HQ2I{0:d}B".format(waveform_len), data)
            # > - Big endian
            # 3H - 3* integer (2 bytes each) - byte order, flds[0]; version, flds[1]; pedestal, flds[2]
            # Q  - integer (8 bytes) - DOM Clock, flds[3]
            # 2i - 2* integer (4 bytes each) - word1, flds[4]; word3, flds[5]
            # {0:d}B - waveform_len integers (1 byte each) - waveform data, flds[6:]
            self.__dom_id = dom_id
            self.__byte_order = flds[0]
            self.__version = flds[1]
            self.__pedestal = flds[2]
            self.__clock_bytes = flds[3]
            self.__word1 = flds[4]
            self.__word3 = flds[5]
            self.__waveform_bytes = flds[6:]

    def __str__(self):
        return "HitSpool@{0:d}[dom {1:012x} clk {2:012x} waveformData*{3:d}]".format(
            self.utime, self.__dom_id, self.domclock, len(self.__waveform_bytes)
        )

    @property
    def dom_id(self):
        return self.__dom_id

    @property
    def domclock(self):
        """Number of DOM Clock cycles

        :return: number of DOM clock cycles
        :rtype: int
        """
        return self.__clock_bytes

    @property
    def byte_order(self):
        return self.__byte_order

    @property
    def version(self):
        return self.__version

    @property
    def pedestal(self):
        return self.__pedestal

    @property
    def word1(self):
        return self.__word1

    @property
    def word3(self):
        return self.__word3

    # word1 - See https://docushare.icecube.wisc.edu/dsweb/Get/Document-20568 for full description of format
    @property
    def hit_size(self):
        return self.word1 & 0x7ff

    @property
    def atwd_chip(self):
        return (self.word1 >> 11) & 0x1

    @property
    def atwd_size(self):
        return (self.word1 >> 12) & 0x03

    @property
    def atwd_available(self):
        return (self.word1 >> 14) & 0x1

    @property
    def fadc_available(self):
        return (self.word1 >> 15) & 0x1

    @property
    def lc(self):
        return (self.word1 >> 16) & 0x03

    @property
    def trigger_word(self):
        return (self.word1 >> 18) & 0xfff

    @property
    def min_bias(self):
        return (self.word1 >> 30) & 0x1

    @property
    def compression_flag(self):
        return (self.word1 >> 31) & 0x1

    # word3 - See https://docushare.icecube.wisc.edu/dsweb/Get/Document-20568 for full description of format
    @property
    def post_peak_count(self):
        return self.word3 & 0x1FF

    @property
    def peak_count(self):
        return (self.word3 >> 9) & 0x1FF

    @property
    def pre_peak_count(self):
        return (self.word3 >> 18) & 0x1FF

    @property
    def peak_sample(self):
        return (self.word3 >> 27) & 0xF

    @property
    def peak_range(self):
        return (self.word3 >> 31) & 0x1

    @property
    def record_bytes(self):
        """Binary representation of record, will only contain waveform data if keep_data = True

        :return: binary representation of record
        :rtype: bytes
        """
        if not self.has_data:
            return self.envelope
        else:
            return self.envelope + self.__data

    @property
    def envelope(self):
        """Binary representation of record envelope

        :return: binary representation of envelope
        :rtype: bytes
        """
        return struct.pack(">2IQ8xQ", self.data_length + HS_ENVELOPE_LENGTH, self.type_id, self.dom_id, self.__utime)

    @property
    def data_bytes(self):
        """Binary representation of record data (all fields other than envelope)

        :return: binary representation of record data
        :rtype: bytearray
        """
        if not self.has_data:
            return None
        else:
            return self.__data

    @property
    def data_length(self):
        """Number of data bytes (number of waveform bytes + 18)

        :return: number of data bytes
        :rtype: int
        """
        if not self.has_data:
            return 0
        else:
            return len(self.__data)

    @property
    def waveform_bytes(self):
        """Binary representation of waveform bytes

        :return: binary representation of waveform data
        :rtype: bytearray
        """
        if not self.has_data:
            return None
        else:
            return self.__data[HS_HEADER_LENGTH:]

    @property
    def waveform_length(self) -> int:
        """Number of waveform bytes

        :return: number of waveform bytes
        :rtype: int
        """
        if not self.has_data:
            return 0
        else:
            return self.data_length - HS_HEADER_LENGTH

    @property
    def has_data(self) -> bool:
        """Indicates if SN Payload contains SN waveform data (true) or not (false)

        :return: indicates if SN payload contains SN waveform data
        :rtype: bool
        """
        return self.__has_data

    @property
    def type_id(self) -> int:
        """IceCube Payload ID (Always 16)

        :return: IceCube HitSpool Payload ID
        :rtype: int
        """
        return HS_TYPE_ID

    @property
    def source_name(self) -> str:
        """Name of IceCube Payload Source

        :return: IceCube payload Source
        :rtype: str
        """
        return "HitSpool Payload"

    @property
    def utime(self) -> int:
        """UTC Timestamp of record since start of year in 0.1ns

        :return: UTC Timestamp of record since start of year in 0.1ns
        :rtype: int
        """
        return self.__utime


class HS_PayloadReader(Reader):
    """Read DAQ payloads from a file"""

    def __init__(self, filename: str, keep_data=True):
        super().__init__(filename, filetype='hitspool', keep_data=keep_data)

    def __next__(self):
        """Read the next payload

        :return: pay
        :rtype: HS_payload
        """
        pay = self.decode_payload(self._fin, keep_data=self._keep_data)
        self._num_read += 1
        return pay

    @classmethod
    def decode_payload(cls, stream, keep_data=True):
        """Decode and return the next payload

        Parameters
        ----------
        stream : io.BinaryIO
            File stream object containing bytes open in binary read mode
        keep_data : bool
            If true, write waveform data to HS_payload, if false waveform data will be written as None
        Returns
        -------
        payload : HS_Payload
            HitSpool Payload
        """
        envelope = stream.read(HS_ENVELOPE_LENGTH)
        if len(envelope) == 0:
            return None

        # Read in payload envelope
        length, type_id, dom_id, utime = struct.unpack(">2IQ8xQ", envelope)
        # > - big endian
        # i - int (4 bytes) - payload length in bytes
        # i - int (4 bytes) - payload type, should be 16
        # q - long long (8 bytes) - DOM ID
        # 8x - Get 8 pad bytes, gets unused (set to zero) bytes from payload
        # q - long long (8 bytes) - UTC timestamp
        if length <= HS_ENVELOPE_LENGTH:
            rawdata = None
        else:
            rawdata = stream.read(length - HS_ENVELOPE_LENGTH)

        return HS_Payload(utime, dom_id, rawdata, keep_data=keep_data)


def read_file(filename, max_payloads) -> None:
    """Read a file of HitSpool Payloads and print them to stdout

    Parameters
    ----------
    filename : str or PathLike
        filename or path to HitSpool data file
    max_payloads : int
        Number of payloads to read
    """
    with HS_PayloadReader(filename) as rdr:
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
