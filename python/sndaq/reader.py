import numbers
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