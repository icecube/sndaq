# SNDAQ - IceCube Supernova DAQ

![tests](https://github.com/icecube/pysndaq/actions/workflows/tests.yml/badge.svg)

A Python port of SNDAQ.

## Installation
SNDAQ can be installed by cloning the repository and running

```bash
python setup.py install
```

Alternatively, for rapid development the command

```bash
python setup.py develop
export sndaq=/path/to/sndaq_folder
```

will install softlinks in your python path to the source in your git checkout. It is recommended that you add the 
second line to your `.bashrc` or similar file.

Conda users can refer to the Conda Docs on 
[Managing Environments](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) 
specifically the section on 
[Setting Environment Variables](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#setting-environment-variables)
to properly configure your environment.
