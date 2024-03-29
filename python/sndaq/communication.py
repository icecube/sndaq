"""Communications between SNDAQ and I3Live. Requires LiveCore
"""
import warnings
import os
import json
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import numpy as np
import datetime
import time

from sndaq.logger import get_logger

logger = get_logger()

from sndaq import get_i3creds

try:
    from livecore.util.misc import unique_id, zmq_ctx
    from livecore.messaging import zmqtransport as zt
    from livecore.messaging.moniclient import ZMQMoniClient
except ImportError as e:
    # If running on any system other than SP(T)S only throw a warning; else, raise an excception
    if not any([host in os.uname()[1].lower() for host in ['spts', 'sps']]):
        warnings.warn("Missing Livecore! sndaq.communications will not be properly initialized")
    else:
        raise ImportError(e) from None

# Certificate validation may fail at SP(T)S, in which case you can bypass it like so:
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


_i3user, _i3pass = get_i3creds()

_default_query_params = {
            'user': _i3user,
            'pass': _i3pass
        }

def get_unique_id():
    """Generate Unique request ID if none is provided
    Returns
    -------
    id : str
    """
    return unique_id()


def query_live_json_view(url, params):
    """Query I3Live databaes via json view.
    NOTE: Requires network connectivity, or a local i3Live server
    For more information on i3Live queries: https://live.icecube.wisc.edu/doc/query/

    Parameters
    ----------
    url :
        URL of I3Live database, e.g. 'https://i3live/run_info/'
    params : dict
        Dictionary of parameters to direct request

    Returns
    -------
    live_data : dict or list of dict
        Dictionary or list of dictionaries containing i3Live response data

    """
    time.sleep(0.1)
    req = Request(url, urlencode(params).encode())
    logger.debug(f"Sending request to {url}: {params}")
    with urlopen(req) as response:
        data = response.read()
    return json.loads(data)


class RunInfoAgent(object):
    """Singleton Live Run Info Retriever
        """
    instance = None

    def __new__(cls, host=None, force=False, *, run_number=None):
        """

        Parameters
        ----------
        host : str
            Host where i3Live service is running
        """
        if host is None and cls.instance is None:
            raise RuntimeError("No Host was provided!")
        elif cls.instance is None:
            cls.instance = super(RunInfoAgent, cls).__new__(cls)
            cls.instance.host = host
            cls.instance.url = f'https://{host}/run_info/'
            cls.instance.run_number = run_number
        elif force:
            cls.instance = None
            return cls.__new__(cls, host, force=True)
        return cls.instance

    def find_run_number(self, timestamp=None):
        """Find the corresponding run number given a timestamp and selection method

        Parameters
        ----------
        timestamp : np.datetime64
            Timestamp around which to perform the search.
            If None, the current run will be returned.

        Returns
        -------
        run_number : int
            The run during which `timestamp` falls, or the current run
        """
        if self.run_number:
            return self.run_number
        # Assumes run duration is ~8 hrs, extra time is added in order to ensure interval includes the correct run
        if timestamp is None:
            start_time = np.datetime64(datetime.datetime.now().date(), 's')  # 's' is required for proper str formatting
            stop_time = np.datetime64(datetime.datetime.now().date(), 's') + np.timedelta64(1, 'D')
        else:
            start_time = timestamp - np.timedelta64(10, 'h')
            stop_time = timestamp + np.timedelta64(10, 'h')

        params = {
            'start': str(start_time).replace('T', ' '),
            'stop': str(stop_time).replace('T', ' ')
        }
        params.update(_default_query_params)
        data = query_live_json_view(self.url, params)
        if not isinstance(data, list):
            data = [data]
        run_numbers = np.array([run['run_number'] for run in data])
        run_starts = np.array([np.datetime64(run['start']) for run in data])
        run_stops = np.array([np.datetime64(run['stop']) for run in data])

        # When requesting info from an ongoing run, run_stop will be None
        if np.datetime64("NaT") in run_stops:
            run_starts = run_starts[run_stops != np.datetime64("NaT")]
            run_stops = run_stops[run_stops != np.datetime64("NaT")]

        idx = np.where((run_starts < timestamp) & (timestamp < run_stops))[0]
        if idx.size == 0:
            raise ValueError(f"Unable to find run matching timestamp {timestamp}, closest matches: {run_numbers}")
        return run_numbers[idx][0]

    def get_run_info(self, run_number):
        """Request information on a run via its run number via json view.

        Parameters
        ----------
        run_number : int
            Number of run for which data is retrieved

        Returns
        -------
        run_info : dict
            Dictionary containing run information
        """
        params = {
            'run_number': run_number
        }
        params.update(_default_query_params)
        return query_live_json_view(self.url, params)


class OfflineMoniClient:
    """Simple class to perform offline "messaging" s.t. moni messages sent to live are re-routed to logs.
    """

    def __init__(self):
        pass

    @staticmethod
    def send_moni(varname, prio, value):
        """Send a monitoring packet to logs. The signature of this function matches LiveMessageSender.send_moni()

        Parameters
        ----------
        varname : str
            i3Live Monitoring variable name
        prio : int
            Priority level
        value : str or dict
            Data transmitted to i3Live (written to Log in this case)
        """
        logger.debug(f"Offline Live Message: varname={varname}, prio={prio} ,value={value}")


class LiveMessageSender(object):
    """Singleton Live message sender object
    """
    instance = None
    _fra_statuses = ['QUEUED', 'IN PROGRESS', 'SUCCESS', 'FAIL']

    def __new__(cls, moni_host=None, moni_port=None, msg=None, offline=False):
        """

        Parameters
        ----------
        moni_host : str
            Host where i3Live service is running
        moni_port : int
            Port on host where i3Live service is running
        """
        if cls.instance is None:
            cls.instance = super(LiveMessageSender, cls).__new__(cls)
            cls.instance.offline = offline
            if not cls.instance.offline:
                cls.instance.sender = ZMQMoniClient(svc='sndaq_fra', moni_host=moni_host, moni_port=moni_port)
            else:
                cls.instance.sender = OfflineMoniClient()
            cls.instance._request_id = None
            cls.msg = msg
        return cls.instance

    @property
    def request_id(self):
        """Request ID - This is a fixed identifier used by i3live to correctly update the associated message
        """
        return self._request_id

    def fra_status(self, status, request_id):
        """Send FRA status update to I3Live

        Parameters
        ----------
        status : str
            Status of FRA request
        request_id :
            Unique request ID for i3live database
        Returns
        -------

        """
        if status not in self._fra_statuses:
            raise ValueError(f"Unknown status {status}, see member `_fra_statuses` for valid values")
        elif status == 'QUEUED':
            err_state = 0
            self._request_id = request_id
        elif status == 'FAIL':
            err_state = 1
            pass  # Do something to send error alongside status update
        elif status == 'SUCCESS':
            err_state = 0
            pass  # Do something to de-register alert with sender
        else:
            err_state = 0

        self.msg.update({'status': status, "request_id": request_id})
        self.sender.send_moni(varname='sndaq_fra_info', prio=2, value=self.msg)
        return err_state

    def fra_result(self, data, request_id):
        """Send FRA result to I3Live

        Parameters
        ----------
        data : dict
            Dictionary of fields in SNDAQ alert handled by alerthandler
        request_id : str
            Unique request ID for i3live database

        Returns
        -------

        """
        self.msg.update({'status': "SUCCESS", "data": data, "request_id": request_id})
        self.sender.send_moni(varname='sndaq_fra_info', prio=2, value=self.msg)
        logger.info("Sent FR result to i3Live")
