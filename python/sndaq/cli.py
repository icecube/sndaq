"""SNDAQ CLI script
"""
import argparse
import sys
import json
import os

from sndaq.analysis import AnalysisConfig
from sndaq.main import launch as launch_sndaq
from sndaq import base_path
from sndaq.logging import get_logger

_no_arg_commands = []
_command_parsers = {}

logger = get_logger()

class _CustomUsageFormatter(argparse.HelpFormatter):
    """Custom formatter to clarify SNDAQ command usage
    """
    def add_usage(self, usage, actions, groups, prefix=None):
        """Adds `sndaq` to beginning of usage string
        """
        if prefix is None:
            prefix = 'sndaq '
        return super(_CustomUsageFormatter, self).add_usage(
             usage, actions, groups, prefix)


def _setup_command_parser(subparsers, name, desc, *, has_args=True):
    """Initialize blank argparse ArgumentParser for SNDAQ command

    Parameters
    ----------
    subparsers : argparse._SubParsersAction
        argparse subparsers object
    name : str
        SNDAQ command name
    desc : str
        SNDAQ command name
    has_args : bool
        Switch for whether command has arguments (True) or not (False)

    Returns
    -------
    parser: argparse.ArgumentParser
        SNDAQ Command argument parser
    """
    parser = subparsers.add_parser(
        name=name,
        prog=name,
        description=desc,
        help=desc,
        formatter_class=_CustomUsageFormatter,
    )
    _command_parsers.update({parser.prog: parser})
    if not has_args:
        _no_arg_commands.append(parser.prog)
    return parser


def _setup_stop_parser(subparsers):
    """Setup SNDAQ `stop` command parser

    Parameters
    ----------
    subparsers : argparse._SubParsersAction
        argparse subparsers object
    """
    name = 'stop'
    desc = 'Stop SNDAQ'
    _setup_command_parser(subparsers, name, desc, has_args=False)


def _setup_process_parser(subparsers):
    """Setup SNDAQ `process` command parser

    Parameters
    ----------
    subparsers : argparse._SubParsersAction
        argparse subparsers object
    """
    name = "process"
    desc = "Process a specific chunk of SN Data"
    choices = ('ccsn', 'merger', 'manual')
    parser = _setup_command_parser(subparsers, name, desc)

    parser.add_argument('t_start', metavar='START_TIME', default=None,
                        help='start time [YYYY-MM-DDThh:mm:ss.fffff]')
    parser.add_argument('t_stop', metavar='STOP_TIME', default=None,
                        help='stop time [YYYY-MM-DDThh:mm:ss.fffff]')
    parser.add_argument('type', choices=choices, metavar='TYPE', default=None,
                        help=f'Request type, choices are: {", ".join(choices)}')
    parser.add_argument('--conf', metavar='CONFIG_FILE', default=None,
                        help='Config. file for additional options')


def _setup_process_json_parser(subparsers):
    """Setup SNDAQ `process-json` command parser

    Parameters
    ----------
    subparsers : argparse._SubParsersAction
        argparse subparsers object
    """
    name = "process-json"
    desc = "Process a chunk of SN Data, specified by a JSON"
    parser = _setup_command_parser(subparsers, name, desc)

    parser.add_argument('json', metavar='JSON', default=None,
                        help='JSON {"use_offsets": "True", "fr_type": "CCSN", ...}')


def _process_json(args):
    """Execute SNDAQ `process-json` command

    Parameters
    ----------
    args : Namespace
        list of arguments produced by argparse

    Notes
    -----
    The following may be used to dump the contents of a json file:
        `sndaq process-json "$(<path/to/somefile.json)"`
    """
    data_json = args.json
    data = json.loads(data_json.replace("'", '"'))

    if data['fr_type'].lower() == 'ccsn':
        logger.debug("Using default ccsn request config")
        ana_conf_path = os.path.join(base_path, 'etc/ccsn_fra.ini')
    elif data['fr_type'].lower() == 'merger':
        logger.debug("Using default merger request config")
        ana_conf_path = os.path.join(base_path, 'etc/merger_fra.ini')
    else:
        logger.warning(f"Unknown configuration '{data['fr_type']}' requested")
        ana_conf_path = os.path.join(base_path, 'etc/analysis.ini')

    if not os.path.exists(ana_conf_path):
        msg = f"Analysis Config `{ana_conf_path}` not found"
        logger.error(msg)
        raise FileNotFoundError(msg)

    ana_conf = AnalysisConfig.from_config(conf_path=ana_conf_path)
    if not ana_conf:
        msg = f"Analysis Config `{ana_conf_path}` is blank"
        logger.error(msg)
        raise ValueError(msg)

    # TODO: Request mfrere that live provide args using SNDAQ config keys
    ana_conf.use_offsets = data['offset_search']
    ana_conf._binsize_ms = data['bin_sizes']
    ana_conf._duration_bgl_ms = data['bg_duration'][0]
    ana_conf._duration_bgt_ms = data['bg_duration'][1]
    ana_conf._duration_exl_ms = data['excl_duration'][0]
    ana_conf._duration_ext_ms = data['excl_duration'][1]

    logger.info("Queued Request for processing")  # TODO Add queueing
    launch_sndaq(ana_conf=ana_conf, conf_path=args.conf_path, request_id=data['request_id'],
                 start_time=data['start_time'], stop_time=data['stop_time'],
                 lightcurve=data['lc_duration'], msg=data, no_run_mode=False, offline_mode=True)


def main():
    parser = argparse.ArgumentParser(
        prog='sndaq',
        description='IceCube Platform for CCSN Analysis'
    )
    subparsers = parser.add_subparsers(
        title='Commands',
        help='SNDAQ Commands',
        dest='command'
    )

    # These functions extend the `subparsers` object in-place
    # They also extend the list `_no_arg_commands` and dict `_command_parsers`
    # This was done for the sake of readability
    _setup_process_parser(subparsers)
    _setup_process_json_parser(subparsers)
    _setup_stop_parser(subparsers)

    # If no arguments or commands are provided, print the top-level help message
    if len(sys.argv) == 1:
        parser.print_help()
        return 0
    # If a commands with arguments is provided *without* arguments, print that command's help message
    elif len(sys.argv) == 2 and sys.argv[1] not in _no_arg_commands + ['-h', '--help']:
        # Catch invalid command options
        try:
            _command_parsers[sys.argv[1]].print_help()
        except KeyError:
            msg = f"Unrecognized Command '{sys.argv[1]}'\nSee `sndaq --help` for available options"
            raise ValueError(msg) from None
        return 0

    args = parser.parse_args()

    #TODO: Figure out how to prevent instancing logger upon calls to help from cli or invalid commands
    # I think this can be done by importing conditionally.
    if args.command == 'stop':
        logger.info('SNDAQ Stopped!')
    elif args.command == 'process-json':
        logger.debug(f'Received `process-json` Command with args: {args}"')
        _process_json(args)
    elif args.command == 'process':
        msg = f"Command `{args.command}` not implemented"
        logger.error(msg)
        raise NotImplementedError()
    else:
        msg = f"Unknown Command: `{args.command}`"
        logger.error(msg)
        raise RuntimeError(msg)


if __name__ == "__main__":
    main()
