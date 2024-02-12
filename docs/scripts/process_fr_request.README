This README contain instructions on modifying your SNDAQ configuration files for performing an offline Fast Response Analysis using IceCube scaler data. This README assumes that you have a working installation of SNDAQ and that you have access to the necessary data. For clarity `base_path` in this context refers to the value obtained from the following

```python
from sndaq import base_path
print(base_path)
```

In the accompanying MWE script `process_fr_request.py`, you may control the time at which the FR trigger is evaluated by modifying the "start_time" field of the request dictionary. Using a timestamp outside the time available in a test data will produce errors (and if it doesn't, please raise an issue). By default, a timestamp in the middle of a 13 minute data set, from SPTS run 267395, is provided.

Locate the default SNDAQ configuration file at `etc/default.ini` relative to the top directory of your SNDAQ installation. Modify the following fields in the file as approriate for your system. You may also copy the file and modify that instead.

```
[i3live]
host = "virgo.icecube.wisc.edu"
...
use_real_run_no = True
run_no = 267395  

[filesystem]
dir_scaler = "/path/to/scaler/data/location"
dir_scaler_bkp =  "/path/to/scaler/data/location/backup/(optional)/"
```

It is expected that the names of the files in your test data set are of the following form

`sn_267395_000005_1412108_1701837.dat`

where the first set of numbers (267395) is the run number, the second set (000005) is the index of this file amongst scaler files generated during this run, the third set (1412108) is the index of the SN payload at the start of the file, and the fourth (1701837) is the index of the SN payload at the end of the file. If you obtain data from a different run, you must modify the configuration field `run_no` appropriately.


If you have modified `default.ini` directly, no further steps are needed. If you have created a new file, you must modify the value of conf_path in the MWE script accordingly.



Alternatively, if you wish to perform a processing request directly from the CLI, you may use the following with an accompanying .json file.

```bash
sndaq process-json "$(<path/to/somefile.json)"
```

To monitor an ongoing processing request, it is recommended that you inspect the main log file located at `log/pysndaq.log` which is regenerated each time a processing run begins. The logs corresponding to the 3 prior processing attempts will be stored under `pysndaq.log.<1,2,3>` and are rotated automatically. Errors and debug messages will be directed to this log file.
