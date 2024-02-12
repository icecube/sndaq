"""MWE script for initiating a fast response (FR) processing run with PySNDAQ
Comparable to invoking `$ sndaq process-json ...`
"""

from sndaq.cli import _process_json
import json
import argparse

req_dict = {"request_id": "<i3Live request ID>",
            "username": "snfrbot",
            "start_time": "2023-08-25 15:16:26.123",  # This falls within SPTS run 267395 test set
            "stop_time": None,
            "fr_type": "CCSN",
            "alert_id": "<human-readable-alert-id-name>",
            "test_request": False,
            "bin_sizes": [500, 1500, 4000, 10000],
            "offset_search": True,
            "bg_duration": [300000, 300000],
            "excl_duration": [15000, 15000],
            "lc_duration": [30000, 60000]
            }
req_json = json.dumps(req_dict)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Copied from sndaq.cli._setup_process_json_parser
    parser.add_argument('json', metavar='JSON', default=None,
                        help='JSON {"use_offsets": "True", "fr_type": "CCSN", ...}')
    args = parser.parse_args([req_json])
    _process_json(args)
