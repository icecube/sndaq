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
setup_keywords['requires'] = ['Python (>2.7.0)']
setup_keywords['zip_safe'] = False
# setup_keywords['use_2to3'] = False
setup_keywords['packages'] = find_packages('python')
setup_keywords['package_dir'] = {'': 'python'}
setup_keywords['cmdclass'] = {'version': SetVersion, 'sdist': DistutilsSdist}


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

#
# Internal data directories.
#
setup_keywords['data_files'] = [('sndaq/data/config', glob('data/config/*'))]
#
# Run setup command.
#
setup(**setup_keywords)
