import os
import gzip
import bz2
import struct
import numpy as np
from sndaq.reader import SN_Payload, SN_MAGIC_NUMBER
from sndaq.detector import Detector


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
        
        i3 = Detector()
        self.doms = i3.dom_table
        

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
    
    def in1d_dom(self, request):
        """
        :param request: Requested strings/DOMs
        :type request: dict
        
        :return: Table of legitimate requested DOMs
        :rtype: array
        """
        cut = np.in1d(self.doms['mbid'], request['doms'])
        return self.doms[cut]
    
    def in1d_str(self, request):
        """
        :param request: Requested strings/DOMs
        :type request: dict
        
        :return: Table of DOMs on legitimate requested strings
        :rtype: array
        """
        cut = np.in1d(self.doms['str'], request['str'])
        return self.doms[cut]
    
    def in1d_combined(self, request):
        """Return all legitimate requested strings/DOMs from dictionary.
        
        :param request: Requested strings/DOMs
        :type request: dict
        
        :return: Table of DOMs on legitimate requested strings/DOMs
        :rtype: array
        """
        req_dom = self.in1d_dom(request)['mbid']
        req_str = self.in1d_str(request)['mbid']
        final = np.unique(np.append(req_dom, req_str))
        cut = np.in1d(self.doms['mbid'], final)
        return self.doms[cut]
    
    def makefile(self, n_scalers = 450, scaler_lambda = 0.8, t0 = 123456789, launch_time = 120006539,
                 filter_type = 'doms', size_limit = 20e3):
        
        # Add dom/string selection function (request functions)
        # Add variable names to returns in doc strings
        # One sentence descriptions in top of doc strings
        
        
        string1 = doms[doms['str']==1]
        dict_filter = {
            'str' : [],
            'doms' : [x for x in string1['mbid']]
        }
    
        doms = dict_filter[filter_type]
        
    
        t0 = int(t0)
        launch_time = int(launch_time)
        n_scalers = int(n_scalers)
        scaler_lambda = int(scaler_lambda)
        utime = int(t0)
        utimes = np.arange(utime, utime + 60*20e4, 20e4)
        size_limit = int(size_limit)

        paylist_init = []

        while self._fout.tell() < size_limit:

            for i, dom in enumerate(string1['mbid']):
                utime = int(utimes[i])
                domclock = (utime - launch_time)//250
                scalers = np.random.poisson(scaler_lambda, size=n_scalers)
                payload = construct_payload(utime, dom, domclock, scalers, True)
                paylist_init.append(payload)
                self.write(payload)
                utimes[i] += (250 * 2**16)*(n_scalers)
        print("File Size:", self._fout.tell()/1000000, "MB")


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
    """Decode and return the next payload.

    :param utime: UTC timestamp from year start
    :type utime: int (8 bytes)

    :param dom_id: DOM mainboard ID
    :type dom_id: int (8 bytes)

    :param domclock: DOM clock cycles elapsed since launch
    :type domclock: int (6 bytes)

    :param scalers: scaler record data
    :type scalers: tuple(int (1 byte))

    :param keep_data:  If true, write scaler data to SN_Payload, if false scaler data will be written as None
    :type keep_data: bool
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
    """Write DAQ payloads to a file"""

    def __init__(self, filename: str, overwrite=False):
        super().__init__(filename, overwrite)

    def write(self, payload):
        self._fout.write(payload.record_bytes)
