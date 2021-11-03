import os
import sys
import gzip
import bz2
import struct
import numpy as np
import configparser
from sndaq.reader import SN_Payload, SN_MAGIC_NUMBER
from sndaq.detector import Detector


class Writer(object):
    """Construct and write payloads to file"""

    def __init__(self, filename: str, overwrite: bool = False):
        """Open a payload file

        :param filename: Name of payload file
        :type filename: str
        """
        if os.path.exists(filename) and not overwrite:
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
        
        self.filesize = 0
        self.n_payloads = 0
        self.n_series = 0
        
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
        """Return all legitimate requested DOMs.
        :param request: Requested strings/DOMs
        :type request: dict
        
        :return dom_table: Table of legitimate requested DOMs
        :rtype dom_table: array
        """
        cut = np.in1d(self.doms['mbid'], request['doms'])
        return self.doms[cut]
    
    def in1d_str(self, request):
        """Return all DOMs on legimate requested strings.
        :param request: Requested strings/DOMs
        :type request: dict
        
        :return dom_table: Table of DOMs on legitimate requested strings
        :rtype dom_table: array
        """
        cut = np.in1d(self.doms['str'], request['str'])
        return self.doms[cut]
    
    def in1d_combined(self, request):
        """Return all legitimate requested DOMs (individual/strings) from dictionary.
        
        :param request: Requested strings/DOMs
        :type request: dict
        
        :return dom_table: Table of DOMs on legitimate requested strings/DOMs
        :rtype dom_table: array
        """
        req_dom = self.in1d_dom(request)['mbid']
        req_str = self.in1d_str(request)['mbid']
        final = np.unique(np.append(req_dom, req_str))
        cut = np.in1d(self.doms['mbid'], final)
        return self.doms[cut]
    
    def makefile(self, n_scalers = 450, scaler_lambda = 0.8, t0 = 123456789, launch_time = 120006539,
                 filter_type = {'doms': None, 'str': None}, payload_step = 20e4, n_hits = None, 
                 pay_series = None, size_limit = 20e3):
        """Create small file with scaler data.
        
        :param n_scalers: Number of scalers 
        :type n_scalers: int
        
        :param scaler_lambda: Scaler poisson distribution
        :type scaler_lambda: float
        
        :param t0: Starting time
        :type t0: int
        
        :param launch_time: DOM launch time
        :type launch_time: int
        
        :param filter_type: Requested DOMs (individual or string)
        :type filter_type: dict
        
        :param n_hits: Number of hits
        :type n_hits: int
        
        :param pay_series: Number of hit series
        :type pay_series: int
        
        :param size_limit: File size limit
        :type size_limit: int
        
        :: payload step
        
        :return new_file: File containing payloads
        :rtype new_file: .dat file
        """
        for key in ['doms', 'str']:
            if key not in filter_type.keys():
                filter_type[key] = None
        final_doms = self.in1d_combined(filter_type)
    
        t0 = int(t0)
        launch_time = int(launch_time)
        n_scalers = int(n_scalers)
        scaler_lambda = float(scaler_lambda)
        utime = int(t0)
        utimes = np.arange(utime, utime + final_doms.size*payload_step, payload_step)
        size_limit = int(size_limit)

        dict_params = {
            't0' : t0,
            'launch_time' : launch_time,
            'n_scalers' : n_scalers,
            'scaler_lambda' : scaler_lambda,
            'start_utime' : utime,
            'current_utimes' : np.arange(utime, utime + final_doms.size*payload_step, payload_step)
        }
        
        config = configparser.ConfigParser()
        config.read('File.ini')
        default = config['DEFAULT']
        payload_step = int(default.getfloat('payload_step'))
        dom_str = [int(x, 10) for x in default.get('dom_str').replace(' ', '').split(',')]
        filter_type = {'str': dom_str}  # Needs to be more dynamic, don't need to assume 'dom_str' is there
        for key in ['doms', 'str']:
            if key not in filter_type.keys():
                filter_type[key] = None
        final_doms = self.in1d_combined(filter_type)
        
        dict_param = {
            't0' : default.getint('t0'),
            'launch_time' : default.getint('launch_time'),
            'n_scalers' : default.getint('n_scalers'),
            'scaler_lambda' : default.getfloat('scaler_lambda'),
            'start_utime' : default.getint('utime'),
            'current_utimes' : np.arange(utime, utime + final_doms.size*payload_step, payload_step)
        }
        
        # Check Mode
        # Check if running from argument mode, config file mode
        # Check for file, change default value to none
        # If all none, go to config mode, if config nothing, error
        # Is there config file?, try with that, backup with arguments
        # Function for each mode (clean coding)
        # Setup for argument, setup for config
        # Anything controllable as argument should be in config file
        
        # Launch times
        # One time for all DOMs to use, or specify start time of every used DOM
        # If don't have appropriate number of launch times for all DOMs, error
        # ToDo: Check ordering scheme for DOM launch times on same string
        
        dict_type = {}
        for val, key in zip([size_limit*1e3, n_hits, pay_series], ['size', 'payloads', 'payload_series']):
            if val is not None:
                dict_type[key] = val
                
        self.write_payloads(dict_type, final_doms, dict_param)
        
    def keep_writing(self, dict_type):
        """Determines if the payloads should continue to be written.
        """
        looping = None
        size_looping = None
        pay_looping = None
        pay_series_looping = None
        if 'size' in dict_type.keys():
            size = dict_type['size']
            if self._fout.tell() < size:
                size_looping = True
            else:
                size_looping = False
        if 'payloads' in dict_type.keys():
            payloads = dict_type['payloads']
            # if creating large data set (multiple files), nrec will reset every time
            if self.n_payloads < payloads:
                pay_looping = True
            else:
                pay_looping = False
        if 'payload_series' in dict_type.keys():
            payload_series = dict_type['payload_series']
            if self.n_series < payload_series:
                pay_series_looping = True
            else:
                pay_series_looping = False 
        l = [x for x in [size_looping, pay_looping, pay_series_looping] if x is not None]
        return all(l)   
    
    def write_payloads(self, dict_type, final_doms, dict_param):
        """Writes payloads until reaching limit.
        """
        itr_dom = iter(enumerate(final_doms['mbid']))
        # try to read list of DOMs, if nothing left, None tuple
        i, dom = next(itr_dom, (None, None))
        # if i/dom are initially None, no DOMs were requested
        if i is None or dom is None:
            raise RuntimeError('No DOMs requested.')
        while self.keep_writing(dict_type):
            # if i/dom are None, move to the next iteration
            # start next hit series
            # initially does nothing, since None tuple would've been chekced above
            if i is None or dom is None:
                itr_dom = iter(enumerate(final_doms['mbid']))
                i, dom = next(itr_dom, (None, None))
            utime = dict_param['start_utime']
            domclock = (utime - dict_param['launch_time'])//250
            scalers = np.random.poisson(dict_param['scaler_lambda'], size=dict_param['n_scalers'])
            payload = construct_payload(utime, dom, domclock, scalers, True)
            self.write(payload)
            self.n_payloads += 1
            self.filesize += 34 + payload.scaler_length
            dict_param['current_utimes'][i] += (250 * 2**16)*(dict_param['n_scalers'])
            # Move to the next iteration, if nothing left, None tuple
            i, dom = next(itr_dom, (None, None))
            # if i/dom are None, hit series complete, increment by 1
            if i is None or dom is None:
                self.n_series += 1
                
    def status(self, sep=' ', end='\n', file=sys.stdout, flush=False):
        """Print summary statement.
        """
        status_string = 'File size: {0} MB, Number of Payloads: {1}, Number of Payload Series: {2}'.format(
            self.filesize/1000000, self.n_payloads, self.n_series)
        print(status_string, sep=sep, end=end, file=file, flush=flush)

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
    # 
