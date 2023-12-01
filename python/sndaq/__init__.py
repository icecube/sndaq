# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
=====
sndaq
=====
IceCube supernova DAQ: search for correlated increases in DOM hits using the
pDAQ 2 ms data stream.
"""

from __future__ import absolute_import
from ._version import __version__
import os
from configparser import ConfigParser

src_path = os.path.realpath(__path__[0])
base_path = os.sep.join(src_path.split(os.sep)[:-2])


def get_i3creds():
    """Get IceCube default credentials (must be initially populated manually by user)

    Returns
    -------
    i3user : str
        Default IceCube Username
    i3pass : str
        Default IceCube Password
    """
    config = ConfigParser()
    config_path = os.path.join(base_path, 'data', 'config', 'i3cred.cfg')

    if os.path.exists(config_path):
        config.read(config_path)
        i3user = config.get('DEFAULT', 'i3user')
        i3pass = config.get('DEFAULT', 'i3pass')
        if 'REDACTED' not in (i3user, i3pass):
            return i3user, i3pass
    else:
        config['DEFAULT'] = {'i3user': 'REDACTED', 'i3pass': 'REDACTED'}
        with open(config_path, 'w') as cred_file:
            config.write(cred_file)
    raise RuntimeError(f"Missing IceCube Default Credentials. Please populate them in {config_path}")