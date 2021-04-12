import bz2
import gzip
import numbers
import os
import struct

SN_TYPE_ID = 16
SN_ENVELOPE_LENGTH = 16
SN_HEADER_LENGTH = 18
SN_MAGIC_NUMBER = 300


class SN_Payload(object):
    TYPE_ID = None
    ENVELOPE_LENGTH = 16

    def __init__(self, utime, data, keep_data=True):
        self.__utime = utime
        if keep_data and data is not None:
            self.__data = data
            self.__has_data = True
        else:
            self.__data = None
            self.__has_data = False

        if self.__has_data:
            scaler_len = len(data) - 18
            flds = struct.unpack(">QHH6B%dB" % (scaler_len,), data)

            self.__dom_id = flds[0]
            self.__clock_bytes = flds[3:9]
            self.__scaler_bytes = flds[9:]

    def extract_clock_bytes(self, clock_bytes):
        if isinstance(clock_bytes, numbers.Number):
            tmpbytes = []
            for _ in range(6):
                tmpbytes.insert(0, clock_bytes & 0xff)
                clock_bytes >>= 8
            return tmpbytes

        if isinstance(clock_bytes, list) or isinstance(clock_bytes, tuple):
            if len(clock_bytes) != 6:
                raise Exception("Expected 6 clock bytes, not " + len(clock_bytes))
            return clock_bytes

        raise Exception("Cannot convert %s to clock bytes" % (type(clock_bytes).__name__,))

    def __str__(self):
        return "Supernova@%d[dom %012x clk %012x scalerData*%d" % \
               (self.utime, self.__dom_id, self.domclock,
                len(self.__scaler_bytes))

    @property
    def domclock(self):
        val = 0
        for byte in self.__clock_bytes:
            val = (val << 8) + byte
        return val

    @property
    def bytes(self):
        if not self.has_data:
            return self.envelope
        else:
            return self.envelope + self.__data

    @property
    def data_bytes(self):
        if not self.has_data:
            return None
        else:
            return self.__data

    @property
    def data_length(self):
        if not self.has_data:
            return 0
        else:
            return len(self.__data)

    @property
    def envelope(self):
        return struct.pack(">2IQ", self.data_length + SN_ENVELOPE_LENGTH, self.type_id, self.__utime)

    @property
    def has_data(self):
        return self.__has_data

    @property
    def type_id(self):
        return SN_TYPE_ID

    @property
    def source_name(self):
        return "Supernova Payload"

    @property
    def utime(self):
        return self.__utime


class PayloadReader(object):
    "Read DAQ payloads from a file"

    def __init__(self, filename, keep_data=True):
        """
        Open a payload file
        """
        if not os.path.exists(filename):
            raise Exception("Cannot read \"%s\"" % filename)

        if filename.endswith(".gz"):
            fin = gzip.open(filename, "rb")
        elif filename.endswith(".bz2"):
            fin = bz2.BZ2File(filename)
        else:
            fin = open(filename, "rb")

        self.__filename = filename
        self.__fin = fin
        self.__keep_data = keep_data
        self.__num_read = 0

    def __enter__(self):
        """
        Return this object as a context manager to used as
        `with PayloadReader(filename) as payrdr:`
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the open filehandle when the context manager exits
        """
        self.close()

    def __iter__(self):
        """
        Generator which returns payloads in `for payload in payrdr:` loops
        """
        while True:
            if self.__fin is None:
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
        "Read the next payload"
        pay = self.decode_payload(self.__fin, keep_data=self.__keep_data)
        self.__num_read += 1
        return pay

    def close(self):
        """
        Explicitly close the filehandle
        """
        if self.__fin is not None:
            try:
                self.__fin.close()
            finally:
                self.__fin = None

    @property
    def nrec(self):
        """Number of payloads read to this point
        """
        return self.__num_read

    @property
    def filename(self):
        """Name of file being read
        """
        return self.__filename

    @classmethod
    def decode_payload(cls, stream, keep_data=True):
        """Decode and return the next payload
        """
        envelope = stream.read(SN_ENVELOPE_LENGTH)
        if len(envelope) == 0:
            return None

        length, type_id, utime = struct.unpack(">iiq", envelope)

        if length <= SN_ENVELOPE_LENGTH:
            rawdata = None
        else:
            rawdata = stream.read(length - SN_ENVELOPE_LENGTH)

        return SN_Payload(utime, rawdata, keep_data=keep_data)


def read_file(filename, max_payloads, write_simple_hits=False):
    if write_simple_hits and filename.startswith("HitSpool-"):
        out = open("SimpleHit-" + filename[9:], "w")
    else:
        out = None

    try:
        with PayloadReader(filename) as rdr:
            for pay in rdr:
                if max_payloads is not None and rdr.nrec > max_payloads:
                    break

                print(str(pay))
                if out is not None:
                    out.write(pay.simple_hit)
    finally:
        if out is not None:
            out.close()


if __name__ == "__main__":
    read_file('../../scratch/data/sn-0.dat', 10)

    # Data file is located at /home/sgriswold/sn-0.dat (04/10/2021)
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

