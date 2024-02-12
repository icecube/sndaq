
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
        Time since start of year measured in 0.1 ns
    """
    return 10*(timestamp.astype('datetime64[ns]') - timestamp.astype('datetime64[Y]')).astype(int)


def utime_to_datetime64(utime, year=np.datetime64('now', 'Y').item().year):
    """Convert timestamp to ns since year start

    Parameters
    ----------
    utime : int
        0.1 ns since the start of the year specified by argument `year`
    year : int
        Year, if None is provided, the current year is assumed
    Returns
    -------
    utime : np.datetime64
        Time since start of year measured in ns
    """
    if isinstance(utime, (list, tuple, np.ndarray)):
        return [np.datetime64(f"{year}", 'Y') + np.timedelta64(t//10, 'ns') for t in utime]
    return np.datetime64(f"{year}", 'Y') + np.timedelta64(utime//10, 'ns')