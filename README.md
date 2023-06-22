# SNDAQ - IceCube Supernova DAQ

![tests](https://github.com/icecube/pysndaq/actions/workflows/tests.yml/badge.svg)

A Python port of SNDAQ, the online pulse-counting software used to search for ~1 s bursts of MeV neutrinos from core-collapse supernovae and other luminous transients. Selected references:
* R. Abbasi et al. (IceCube Collaboration), [IceCube sensitivity for low-energy neutrinos from nearby supernovae](https://doi.org/10.1051/0004-6361/201117810), A&A 535:A109, 2011.
* V. Baum, B. Eberhardt, A. Fritz, D. Heereman and B. Riedel, [Recent improvements in the detection of supernovae with the IceCube observatory](https://doi.org/10.22323/1.236.1096), PoS(ICRC2015) 1096, 2016.
* S. Griswold, [End-to-End Tests of the Sensitivity of IceCube to the Neutrino Burst from a Core-Collapse Supernova](https://pos.sissa.it/395/1085/), PoS(ICRC2021) 1085, 2021.

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
