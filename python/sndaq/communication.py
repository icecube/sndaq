"""Communications between SNDAQ and I3Live. Requires LiveCore
"""
import warnings
import os
import json

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


class LiveMessageSender(object):
    """Singleton Live message sender object
    """
    instance = None
    _fra_statuses = ['QUEUED', 'IN PROGRESS', 'SUCCESS', 'ERROR']

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
            cls.instance.sender = ZMQMoniClient(svc='sndaq_fra', moni_host=moni_host, moni_port=moni_port)
            cls.instance.request_id = None
        return cls.instance

    def fra_status(self, status, request_id=None):
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
            if request_id is None:
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
        self.sender.send_moni(varname='sndaq_fra_status', prio=2, value={"status": status, "request_id": request_id})
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
        self.sender.send_moni(varname='sndaq_fra_result', prio=3, value={"data": data, "request_id": request_id})
