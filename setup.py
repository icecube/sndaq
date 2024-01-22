#!/usr/bin/env python
#
# Licensed under a 3-clause BSD style license - see LICENSE.rst
from __future__ import absolute_import, division, print_function
#
# Standard imports
#
from glob import glob
import os
import re
#
from distutils.command.sdist import sdist as DistutilsSdist
from setuptools import setup, find_packages
#
from python.sndaq._git import get_version, SetVersion
from python.sndaq import base_path

from pybind11.setup_helpers import Pybind11Extension

if not os.path.exists(os.path.join(base_path, 'log')):
    os.mkdir(os.path.join(base_path, 'log'))
#
# Begin setup
#
setup_keywords = dict()
#
setup_keywords['name'] = 'sndaq'
setup_keywords['description'] = 'IceCube supernova simulation package'
setup_keywords['author'] = 'IceCube Collaboration'
setup_keywords['author_email'] = 'sn-wg@icecube.wisc.edu'
setup_keywords['license'] = 'BSD'
setup_keywords['url'] = 'https://github.com/icecube/pysndaq'
setup_keywords['version'] = get_version()
setup_keywords['entry_points'] = {'console_scripts': ['sndaq = sndaq.cli:main']}
#
# Use README.md as a long_description.
#
setup_keywords['long_description'] = ''
if os.path.exists('README.md'):
    with open('README.md') as readme:
        setup_keywords['long_description'] = readme.read()
#
# Set other keywords for the setup function.
#
if os.path.isdir('bin'):
    # Treat everything in bin as a script to be installed.
    setup_keywords['scripts'] = \
    [fname for fname in glob(os.path.join('bin', '*'))]
setup_keywords['provides'] = [setup_keywords['name']]
setup_keywords['requires'] = ['Python (>3.6.0)']
setup_keywords['zip_safe'] = False
setup_keywords['packages'] = find_packages('python')
setup_keywords['package_dir'] = {'': 'python'}
setup_keywords['cmdclass'] = {'version': SetVersion, 'sdist': DistutilsSdist}
setup_keywords['test_suite'] = 'sndaq.tests.sndaq_test_suite.sndaq_test_suite'

requires = []
optionals = {}
with open('requirements.txt', 'r') as f:
    for line in f.readlines():
        package = line.strip()
        if not package:
            continue
        # Check for extra requirement specifications
        extra_spec = re.search('\[(.*?)]', package)
        if not extra_spec:
            requires.append(package)
        else:
            extra_spec = extra_spec.group()
            # Removes '[]' characters, obtains all spec. keys and removes spaces in spec. keys
            spec_keys = [s.strip(' ') for s in re.sub('\[|]', '', extra_spec).split(',')]
            package = package.replace(extra_spec, '')
            for spec_key in spec_keys:
                if spec_key not in optionals:
                    optionals.update({spec_key: [package]})
                else:
                    optionals[spec_key].append(package)

setup_keywords['install_requires'] = requires
setup_keywords['extras_require'] = optionals

setup_keywords['ext_modules'] = [
    Pybind11Extension(
        "rebin",
        sorted(glob("sndaq/util/*.cpp")),
    ),
]

#
# Internal data directories.
#
setup_keywords['data_files'] = [('sndaq/data/config', glob('etc/*.ini'), glob('etc/geometry/*.txt'), glob('etc/effvol/*.txt')]
#
# Run setup command.
#
setup(**setup_keywords)
