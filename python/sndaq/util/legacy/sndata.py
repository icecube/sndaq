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

_url_run_info = "https://hercules.icecube.wisc.edu/run_info/"
_url_moni = "https://hercules.icecube.wisc.edu/moni_access/"


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

    def cands_from_log(lines):
        """Simple filter to obtain candidate info from raw text of log file
        """
        for line in lines:
            if b'candidate' in line:
                yield line

    data = []
    for line in cands_from_log(lines):
        rgx_trigger_no = "(?<=New\ trigger \#)[0-9]+"
        rgx_cand_no = "(?<=candidate \#)[0-9]+"
        rgx_ana_no = "(?<=Analysis \#)[0-9]+"
        rgx_xi = "(?<=S=)[0-9].[0-9]+"
        rgx_time = "(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"

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