"""Communications between SNDAQ and I3Live. Requires LiveCore
"""
import warnings
import os
import json

try:
    from livecore.misc.util import unique_id, zmq_ctx
    from livecore.messaging import zmqtransport as zt
    from livecore.messaging.moniclient import ZMQMoniClient
except ImportError as e:
    # If running on any system other than SP(T)S only throw a warning; else, raise an excception
    if not any([host in os.uname()[1].lower() for host in ['spts', 'sps']]):
        warnings.warn("Missing Livecore! sndaq.comms will not be properly initialized")
    else:
        raise ImportError(e) from None


class LiveMessageSender(object):
    """Singleton Live message sender object
    """
    instance = None
    _fra_statuses = ['QUEUED', 'IN PROGRESS', 'SUCCESS', 'FAIL']

    def __new__(cls, moni_host=None, moni_port=None):
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
            cls.instance.sender = zmq_ctx(ZMQMoniClient, svc='sndaq_fra', moni_host=moni_host, moni_port=moni_port)
            cls.instance.request_id = None
        return cls.instance

    def fra_status(self, status, id=None):
        """Send FRA status update to I3Live

        Parameters
        ----------
        status : str
            Status of FRA request
        id :
            Unique request ID for i3live database
        Returns
        -------

        """
        if status not in self._fra_statuses:
            raise ValueError("Unknown status {status}, see member `_fra_statuses` for valid values")
        elif status == 'QUEUED':
            if id is None:
                self.request_id = unique_id()
            err_state = 0
        elif status == 'FAILED':
            err_state = 1
            pass  # Do something to send error alongside status update
        elif status == 'SUCCESS':
            err_state = 0
            pass  # Do something to de-register alert with sender
        else:
            err_state = 0
        self.sender.send_moni(varname='sndaq_fra_status', prio=3, value=status, id=id)
        return err_state


    def fra_result(self, data, id):
        """Send FRA result to I3Live

        Parameters
        ----------
        data : dict
            Dictionary of fields in SNDAQ alert handled by alerthandler
        id : str
            Unique request ID for i3live database

        Returns
        -------

        """
        self.sender.send_moni(varname='sndaq_fra_result', prio=3, value=data, id=id)
