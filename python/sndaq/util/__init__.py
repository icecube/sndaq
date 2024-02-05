
import numpy as np


def datetime64_to_utime(timestamp):
    """Convert timestamp to ns since year start

    Parameters
    ----------
    timestamp : np.datetime64
        UTC timestamp to convert to utime
    Returns
    -------
    utime : int
        Time since start of year measured in ns
    """
    return (timestamp - np.datetime64(f"{timestamp.item().year}", 'Y')).astype('timedelta64[ns]').astype(int)


def utime_to_datetime64(utime, year=np.datetime64('now', 'Y').item().year):
    """Convert timestamp to ns since year start

    Parameters
    ----------
    utime : int
        ns since the start of the year specified by argument `year`
    year : int
        Year, if None is provided, the current year is assumed
    Returns
    -------
    utime : int
        Time since start of year measured in ns
    """
    if isinstance(utime, (list, tuple, np.ndarray)):
        return [np.datetime64(f"{year}", 'Y') + np.timedelta64(t, 'ns') for t in utime]
    return np.datetime64(f"{year}", 'Y') + np.timedelta64(utime, 'ns')