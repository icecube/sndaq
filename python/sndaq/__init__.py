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

src_path = os.path.realpath(__path__[0])
base_path = os.sep.join(src_path.split(os.sep)[:-2])
