.. _installation:

Installing SNDAQ
================

Offline Installation
--------------------

SNDAQ can be installed on a laptop or in a directory on the WIPAC cluster by cloning the `SNDAQ GitHub repository <https://github.com/icecube/sndaq>`_ and running

.. code-block:: bash

   python setup.py install

Alternatively, for rapid development the command

.. code-block:: bash

   python setup.py develop
   export sndaq=/path/to/sndaq_folder

will install softlinks in your python path to the source in your git checkout.  It is recommended that you add the second line to your ``.bashrc`` or an equivalent file.

Online Installation: SPTS (WIPAC)
---------------------------------

.. note::

  Several of these steps refer to the C++ version of SNDAQ and are about to
  become obsolete.

Installing SNDAQ on the South Pole Testing System (SPTS) at WIPAC or the South
Pole System (SPS) at Pole requires use of a script based on `fabric
<https://www.fabfile.org/>`_. A ``fabric`` script is prepared for each release
of the SNDAQ software. You will need to request an account on SPTS by emailing
the IceCube Help Desk at `help@icecube.wisc.edu
<mailto:help@icecube.wisc.edu>`_.

Installing the fabric script
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To begin, log into the machine ``access`` and run your commands as the ``pdaq``
user:

.. code-block:: bash

  ssh username@msn-sps.icecube.wisc.edu
  ssh username@spts-access
  sudo su - pdaq
  cd /software/stage/sndaq

Next, check if the SNDAQ fabric script is already installed by running a
directory listing. If it is installed, you should see something like this:

.. code-block:: bash

  pdaq@access[sndaq] $ ls -l sndaq-fabric
  total 224
  -rw-rw-r-- 1 pdaq pdaq  4160 Jun 30  2019 fabfile.cfg
  -rw-rw-r-- 1 pdaq pdaq 44893 May 10  2020 fabfile.py

You will want to update the fabric script to the latest version:

.. code-block:: bash

  cd /software/stage/sndaq/sndaq-fabric/
  git pull origin master

Stage SNDAQ
^^^^^^^^^^^

If the fabric script is installed and ready to go, you can continue staging.
Otherwise, clone the fabric script from the WIPAC GitHub repository:

.. code-block:: bash

  cd /software/stage/sndaq
  git clone git@github.com:WIPACrepo/sndaq-fabric.git

Once the fabric scripts are prepared, SNDAQ can be staged on ``spts-access``.
The staging follows these steps:

#. Look for a matching already staged version of SNDAQ.
#. Check out the requested branch or tag into ``/software/stage/sndaq/sndaq-git``.
#. Run CMake to prepare the build.
#. Build SNDAQ into ``/software/stage/sndaq/sndaq_[name]/build``.
#. Run CMake install into ``/software/stage/sndaq/sndaq_[name]/install``. Then perform the stage using the fabric script:

   .. code-block:: bash

      cd /software/stage/sndaq/sndaq-fabric
      fab stage:version=[name]

   Here you would replace ``[name]`` with the code name of a particular
   software release. For example, to install **Beer_TrooperXIII** released in
   August 2016, run

   .. code-block:: bash

      cd /software/stage/sndaq/sndaq-fabric
      fab install:version=Beer_TrooperXIII

  If you are installing an SNDAQ branch rather than a named release, prepend
  the branch name with the string ``branches//``. For example, stage a branch
  by running

  .. code-block:: bash

      cd /software/stage/sndaq/sndaq-fabric
      fab install:version=branches/[branch_name]

Deploy SNDAQ
^^^^^^^^^^^^

If the stage is successful, you can then deploy SNDAQ to ``2ndbuild``. Some of
the steps performed by the deploy procedure are:

* Stop the running SNDAQ instance.
* Copy the SNDAQ installation and its dependencies from ``spts-access`` to ``2ndbuild``.
* Copy helper scripts to ``expcont``.
* Set up cronjobs on ``2ndbuild`` and ``expcont``.

The deploy procedure does not start SNDAQ after completion. To do this,
assuming deployment was first successful, run the command

  .. code-block:: bash

     fab start

SNDAQ should now be installed and running. 

.. note::

   Sometimes the fabric script will raise an exception stating that it timed
   out registering SNDAQ with `i3live <https://live.icecube.wisc.edu>`_. If
   this occurs, try restarting SNDAQ one or two times with ``fab start``. If
   the timeout persists, consult **[SECTION XYZ]** on troubleshooting the live
   system.

Online Installation: SPS (South Pole)
-------------------------------------

Installing SNDAQ on SPS is identical to the instructions given in the previous
section, except that you replace the machine ``spts-access`` with
``sps-access``.

Prior to installing SNDAQ on SPS, submit a `Non-Standard Operations Request <https://docs.google.com/forms/d/e/1FAIpQLSdMBrcyz6HAAHdiQlfoNKterHHZUXfMnbIsy27BdNaPTyuN1w/viewform?hl=de\&formkey=dDhBMGo4TWVsWElzTTlGUTlQQ3FYZHc6MQ\#gid=0>`_
and schedule a time window for the installation. Choose a time window such that
the `TDRSS or DSCS
<https://internal.icecube.wisc.edu/satellite/riseset_table.php>`_ satellites
will be online and visible at Pole.

  .. note::

   Make sure your Operations Request is submitted at least three days in advance of your intended installation so that the Winterovers will be available to help in case the connection drops during installation or something else goes wrong that requires the release to be rolled back.
