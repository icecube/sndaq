"""Readers for SNDAQ BT20 Output ROOT files, a.k.a. "SN Data"
"""

from time import sleep
import requests
import json
import os
import re
import tarfile
import uproot
import datetime as dt
import numpy as np

# Change virgo to live when running in production
_url_run_info = "https://virgo.icecube.wisc.edu/run_info/"
_url_moni = "https://virgo.icecube.wisc.edu/moni_access/"


def check_cred(USER, PASS):
    """Checks that a set of IceCube LDAP credentials can be found.
    If None are provided, this will attempt to load them from Env. Vars before raising an error
    This does not check the validity of the IceCube Credentials.

    Parameters
    ----------
    USER : str
        IceCube LDAP username
    PASS : str
        IceCube LDAP Password

    Returns
    -------
    CRED : tuple of str
        Tuple of validated
    Raises
    -------
    ValueError : If None is provided for either `user` or `pass
    """
    if None in (USER, PASS):
        try:
            if USER is None:
                USER = os.environ['I3USER']
            if PASS is None:
                PASS = os.environ['I3PASS']
        except KeyError:
            raise ValueError("Missing Login credentials. Please provide valid IceCube LDAP credentials"
                             "or define them under Env. Variables `I3USER` and `I3PASS`")
    return USER, PASS


def get_run_info(run_no, USER=None, PASS=None):
    """Get Run Info from I3Live via JSON query

    Parameters:
    -----------
    run_no : int
        pDAQ Run number, see https://live.icecube.wisc.edu/auth/?next=/recent/
        for recent Runs. Only runs for which processing has completed are valid
    USER : str
        IceCube username to initiate Request to i3live
    PASS : str
        IceCube pass to initiate Request to i3live

    Returns
    -------
    data : dict
        Dictionary containing run information
    """
    USER, PASS = check_cred(USER, PASS)

    DATA = {
        'user': USER,
        'pass': PASS,
        'run_number': run_no
    }

    sleep(1)  # Required to prevent accidental DDoS
    response = requests.post(url=_url_run_info, data=DATA)
    data = json.loads(response.text)
    return data


def get_cands_from_live(run_no, USER=None, PASS=None):
    """Get SN candidates from i3Live via JSON query

    Parameters
    ----------
    run_no : int
        Run number from which to obtain the candidates
    USER : str
        IceCube username to initiate Request to i3live
    PASS : str
        IceCube pass to initiate Request to i3live

    Returns
    -------
    data : dict
        Dictionary of candidates from i3Live
    """
    USER, PASS = check_cred(USER, PASS)
    fmt = "%Y-%m-%d %H:%M:%S"

    run_info = get_run_info(run_no, USER, PASS)
    START = dt.datetime.strptime(run_info['start'], fmt)
    STOP = dt.datetime.strptime(run_info['stop'], fmt) + dt.timedelta(minutes=10)

    # See https://live.icecube.wisc.edu/doc/query/ for more info
    SERVICE = "sndaq"  # i3Live service Name
    VARNAME = "significance"  # Service Variable name

    DATA = {
        'user': USER,
        'pass': PASS,
        'start': START,
        'stop': STOP,
        'src_type': 'simple',  # Required for some variables
        'service': SERVICE,
        'varname': VARNAME
    }

    sleep(1)  # Required to prevent accidental DDoS
    response = requests.post(url=_url_moni, data=DATA)
    data = json.loads(response.text)

    def filter_cand(cand):
        """Filter out candidates from within 5 minutes of run start (non-triggerable)"""
        return dt.datetime.strptime(cand['time'], fmt + '.%f') - START < dt.timedelta(minutes=5)

    data = [cand for cand in data if not filter_cand(cand)]

    return data


def get_cands_from_log(run_no, USER_live, PASS_live, USER_ldap=None, cache_dir='./', debug=False):
    """Get SN candidates from SNDAQ log files (Requires LDAP credentials)
    This function will attempt to download from the data warehouse

    Parameters
    ----------
    run_no : int
        Run number from which to obtain the log
        See /data/exp/IceCube/monitoring/sn/... for SNDAQ logs
    USER_live : str
        IceCube LDAP username for i3Live request
    PASS_live : str
        IceCube LDAP Password for i3Live request
    USER_ldap : str
        IceCube LDAP username for data warehouse access
    cache_dir : str or PathLike
        Location to store SNDAQ log file locally
        Defaults to current directory
    debug : bool
        Switch to return Log file contents as well as candidates

    Returns
    -------
    data : list of dict
        List of candidate descriptions from SNDAQ logfile
    """
    run_info = get_run_info(run_no, USER_live, PASS_live)
    fmt = "%Y-%m-%d %H:%M:%S"
    run_start = dt.datetime.strptime(run_info['start'], fmt)
    run_stop = dt.datetime.strptime(run_info['stop'], fmt)

    log_path_remote = f"/data/exp/IceCube/{run_start.year}" \
                      f"/monitoring/sn/{run_stop.month:02d}{run_stop.day:02d}" \
                      f"/sndaq_{run_no}.tar.gz"

    log_path_local = os.path.join(cache_dir, f"{os.path.basename(log_path_remote)}")
    if not os.path.exists(os.path.dirname(log_path_local)):
        raise FileNotFoundError(f"Missing Directory: {os.path.dirname(log_path_local)}")

    # If the Log file is not found locally, attempt to download it
    if not os.path.exists(log_path_local):
        print(f"Attempting to download SNDAQ Logfile: {os.path.basename(log_path_remote)}")
        cmd_str = f'rsync -PavL {USER_ldap}@data.icecube.wisc.edu:' \
                  f'{log_path_remote} {log_path_local}'
        os.system(cmd_str)

    with tarfile.open(log_path_local, 'r') as tf:
        with tf.extractfile(f'sndaq_{run_no}.log') as log_file:
            lines = log_file.readlines()

    def cands_from_log(_lines):
        """Simple filter to obtain candidate info from raw text of log file
        """
        for _line in _lines:
            if b'candidate' in _line or b'Significance corrected' in _line:
                yield _line

    data = []
    rgx_trigger_no = "(?<=New\ trigger \#)[0-9]+"
    rgx_cand_no = "(?<=candidate \#)[0-9]+"
    rgx_ana_no = "(?<=Analysis \#)[0-9]+"
    rgx_xi = "(?<=S=)[0-9].[0-9]+"
    rgx_xi_corrected = "(?<=Significance corrected from: )[0-9].[0-9]+ to [0-9].[0-9]+"  # Combined with check for 'Significance corrected' below
    rgx_time = "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
    for line in cands_from_log(lines):
        if b'Significance corrected' not in line:
            trigger_no = int(re.search(rgx_trigger_no, line.decode()).group())
            cand_no = int(re.search(rgx_cand_no, line.decode()).group())
            ana_no = int(re.search(rgx_ana_no, line.decode()).group())
            xi = float(re.search(rgx_xi, line.decode()).group())
            log_time, trigger_time = re.findall(rgx_time, line.decode())

            current = {
                'time': np.datetime64(trigger_time),
                'xi': xi,
                'ana': ana_no,
                'trigger': trigger_no,
                'cand': cand_no
            }

            if data and data[-1]['cand'] == cand_no:
                data[-1].update(current)
            else:
                data.append(current)

        else:  # Assumes correction messages always follows trigger messages, and that no new candidates form between
            # Trigger window closing (trigger becomes finalized) and when correction completes.
            xi, xip = (float(x) for x in re.search(rgx_xi_corrected, line.decode()).group().split(' to '))
            if abs(data[-1]['xi'] - xi)/xi > 1e-4:
                raise ValueError(f'Error getting corrected xi for candidate {data[-1]["cand"]}')
            data[-1].update({'xip': xip})

    if debug:
        return data, lines

    return data


def get_cands_from_sndata(sndata_path):
    """ Get SN candidates from SNDAQ Data file (ROOT)
    This assumes the SN Data file exists locally, code to automatically grab data is not provided
    as these files can approach 2 GB in size. See /data/exp/IceCube/<year>/internal-system/...
    in the data warehouse for SN data files.

    Parameters:
    -----------
    sndata_path : str or PathLike
        Path to SNDAQ data file

    Returns:
    --------
    data : list of dict
        List of candidate descriptions from SN Data file:
        NOTE: "Corr" in dictionary keys means "Correlated" with the SMT8 rate,
              Significances contained by these elements are "Uncorrected"
        NOTE: "DeCorr" in dictionary keys means "Decorrelated" with the SMT8 rate
              Significances contained by these elements are "Corrected"
        NOTE: "_500" indicates that the SMT8 rates have been rebinned to 500 ms.
    """
    data = []
    with uproot.open(sndata_path) as f:
        for key, val in f.items():
            if 'SigniNChan' in key:
                # Find decimal characters (\d+) following string 'Cand'
                rgx_n_cand = '(?<=Cand)\d+'
                idx_cand = int(re.search(rgx_n_cand, key).group()) - 1

                # Find alphabetical characters following string 'SigniNChan' and
                # Find numerical characters following a '_' character
                rgx_cand_type = '(?<=SigniNChan)[a-z,A-Z]+|\_[0-9]+'
                cand_type = ''.join(re.findall(rgx_cand_type, key))

                if not data or data[-1]['cand_no'] != idx_cand+1:
                    data.append({"cand_no": idx_cand+1})
                data[idx_cand].update({cand_type: val})
    return data


def _sndaq_median(x):
    """Obtain Median of an array exactly as SNDAQ would.
    This mimics the behavior of ROOT TMath::Median()

    Parameters
    ----------
    x : numpy.ndarray
        Array for which to find the median

    Returns
    -------
    median : float
        Median of array x

    Notes
    -----
    The documentation for TMath::Median() indicates
    that for an array with size n>1000, the single element
    x[x.size//2] is returned in all cases. This is not
    reflected by the implementation of TMath::Median().
    (Currently, ROOT v6.26)
    """
    y = np.sort(x)
    n = x.size
    idx = n // 2

    if n % 2:
        return y[idx]
    else:
        return y[idx - 1:idx + 1].mean()


def _sndaq_mad(x):
    """Obtain Median Absolute Deviation (MAD) of an array
    exactly as SNDAQ would. This mimics the behavior of
    SNDAQ's Sni3TriggerSubtractor::getMAD()

    Parameters
    ----------
    x : numpy.ndarray
        Array for which to find the MAD

    Returns
    -------
    mad : float
        Median absolute deviation of array x

    Notes
    -----
    SNDAQ's Implementation of this calculation
    erroneously pads the input array with one 0 element.
    This has been incldued for the sake of comparison
    """
    median = _sndaq_median(x)
    y = np.append(x, [0])
    return _sndaq_median(np.abs(y - median))


def combine_cands(cand_live, cand_log, cand_data, *, tol=1e-5, verbose=False, use_sndaq_method=True):
    """Combine candidates obtained from the available legacy sources

    Parameters
    ----------
    cand_live : list of dict
        Dictionaries containing SN candidates from Live
    cand_log :list of dict
        Dictionaries containing SN candidates from SNDAQ logs
    cand_data : list of dict
        Dictionaries containing SN candidates from SN Data
    tol : float
        Tolerance for difference between SN candidate significances
    verbose : bool
        If True, include debug information on candidates in disagreement between sources
    use_sndaq_method : bool
        If True, compute median and MAD using SNDAQ methods
        If False, compute median and MAD using pure numpy

    Returns
    --------
    data : list of dict
        List of dictionaries containing summary information on candidates

    See Also
    --------
    sndaq.util.legacy.get_cands_from_live
    sndaq.util.legacy.get_cands_from_log
    sndaq.util.legacy.get_cands_from_sndata
    """
    try:
        assert (len(cand_live) == len(cand_log) == len(cand_data))
    except AssertionError:
        raise ValueError("Candidate sources must have equal lengths,"
                         f"given ({len(cand_live)} {len(cand_log)} {len(cand_data)}")
    data = []
    for i, (live, log, sndata) in enumerate(zip(cand_live, cand_log, cand_data)):
        xi_live = live['value']
        xi_log = log['xi']
        xip_log = log['xip']

        rate, signi = sndata['Corr'].values()
        idx_sndaq = np.abs(signi - xi_log).argmin()
        xi_sndaq = signi[idx_sndaq]

        # Log contains 'correct' xi_prime, neither of the other sources do
        diff_live = np.abs((xi_log - xi_live) / xi_log)
        diff_sndaq = np.abs((xi_log - xi_sndaq) / xi_log)

        try:
            assert (diff_live < tol and diff_sndaq < tol)
        except AssertionError:
            raise ValueError(f"Candidate {i:<3d} has differing xi greater than tolerance!")

        if use_sndaq_method:
            median = _sndaq_median(rate)
            mad = _sndaq_mad(rate)
        else:
            median = np.median(rate)
            mad = np.median(np.abs(rate - median))

        lower = median - 3 * mad
        upper = rate.max() + 1
        cut = (lower <= rate) & (rate < upper)

        a, b = np.polyfit(rate[cut], signi[cut], deg=1)
        xip_sndaq = xi_sndaq - a * rate[idx_sndaq] - b

        try:
            diff_xip = np.abs((xip_sndaq - xip_log) / xip_log)
            assert (diff_xip < tol)
        except AssertionError:
            if verbose:
                err_str = '\n'.join([
                    f"Cand #{i + 1} (Ana.{log['ana']}) \tdelta_xip={diff_xip:8.6f} ({diff_xip * 100:5.3f}%)",
                    f" n={signi.size} ({signi[cut].size})\n median={median}\n MAD={mad}\n bounds={(lower, upper)}",
                    f" a={a:10.7f}\n b={b:10.7f} \n rate={rate[idx_sndaq]}",
                    f"{'':^12}|{'xi':^12}|{'xi_prime':^12}",
                    '-' * 39,
                    f"{'sndaq':^12}| {xi_sndaq:10.5f} | {xip_sndaq:10.5f}",
                    f"{'log':^12}| {xi_log:10.5f} | {xip_log:10.5f}",
                    '-' * 39
                ])
                print(err_str)

            # If not using the SNDAQ method, the majority of candidates will differ from their reported values
            # by an amount greater than the default tolerance. This is due partly b/c SNDAQ's calculation was performed
            # in error since BT13 (2015). For now (04/27/2023), don't error if this check fails
            if use_sndaq_method:  # TODO: Clean this up
                raise ValueError(f"Candidate {i:<3d} has differing xi' greater than tolerance!\n")

        data.append({
            'trigger_time': log['time'],
            'nh_alert_time': live['time'],
            'xi': xi_sndaq,
            'xip': xip_sndaq,
            'ana': log['ana'],
            'trigger_no': log['trigger'],
            'cand_no': log['cand'],
            'muon_rates': rate.astype(np.uint16)
        })
    return data


def n_ana_to_desc(n):
    """Convert SNDAQ Analysis Number to binsize (+offset) description

    Parameters
    ----------
    n : int
        SNDAQ Analysis number

    Returns
    -------
    desc : string
        SNDAQ Analysis description
    """
    if n == 1:
        binning = 0.5
        offset = 0
    elif n in range(2, 5):
        binning = 1.5
        offset = (n - 2) * 0.5
    elif n in range(5, 13):
        binning = 4.0
        offset = (n - 5) * 0.5
    elif n in range(13, 33):
        binning = 10
        offset = (n - 10) * 0.5
    else:
        raise ValueError('Unrecognized Analysis number, expected n in [1, 32]')
    return f"{binning:>4.1f} s (+{offset:<3.1f} s)"