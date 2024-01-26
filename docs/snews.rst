.. _snews:

SuperNova Early Warning System
==============================

Overview
--------

The `SuperNova Early Warning System <https://snews2.org/>`_, or SNEWS, is a
network of neutrino and dark matter detectors sensitive to the 1-100 MeV
neutrinos produced by a nearby core-collapse supernova. The network provides
the astronomical community with a high-confidence early warning of a supernova
hours to days before the optical emission can be detected. It provides timing
and pointing information about the supernova for optical follow-up.

Further information about SNEWS and its successor network, SNEWS 2.0, can be
found here:

* P. Antonioli et al., `SNEWS: The Supernova Early Warning System * <http://dx.doi.org/10.1088/1367-2630/6/1/114>`_, New J. Phys. 6:114, 2004.
* S. Al Kharusi et al., `SNEWS 2.0: a next-generation supernova early warning system for multi-messenger astronomy <https://doi.org/10.1088%2F1367-2630%2Fabde33>`_, New J. PHys. 23:3, 2021.

In the following sections, we describe the interaction between IceCube, the
original SNEWS network, and SNEWS 2.0.

SNEWS
-----

The SNDAQ Virtual Machine
^^^^^^^^^^^^^^^^^^^^^^^^^

A virtual machine (VM) hosted at WIPAC handles communications between IceCube
and the SNEWS server, hosted at Brookhaven National Laboratory. To get access
to the VM, `contact the IceCube Help Desk <mailto:help@icecube.wisc.edu>`_ to
be added to the list of users. Once your are added, you can log into the
machine through the ``cobalt`` nodes:

.. code:: bash

  $> ssh yourname@pub.icecube.wisc.edu
  $> ssh cobalt
  $> ssh sndaq
  $> sudo su - sndaq

Alerts from i3Live
^^^^^^^^^^^^^^^^^^

If SNDAQ measures a test statistic which fulfills the criteria for sending an
alert to SNEWS, it forwards the information vie i3Live and i3Live publishes the
SNEWS alert information via ``zmq``. The script

.. code-block:: bash

  sndaq@sndaq:~$/I3LiveListener/startI3LiveListener.py

runs continuously and checks the published i3Live tables for SNEWS alerts. The
snippet below shows part of the program, which handles the ``zmq`` connection
to i3Live:

.. code-block:: python

  context = zmq.Context()
  socket = context.socket(zmq.SUB)
  socket.connect("tcp://live:7010")
  socket.setsockopt(zmq.SUBSCRIBE, '')

I3Live also publishes test alerts every 6 hours. If it receives a SNEWS alert
or a test alert, it decides whether or not to send test alerts to the SNEWS

.. code-block:: bash

  sndaq@sndaq:~$/I3LiveListener/make_alerts_html.py

SNEWS monitoring webpages are available in the SNDAQ section of i3Live. The
shell scripot

.. code-block:: bash

  sndaq@sndaq:~$/I3LiveListener/checkI3LiveListener.sh

polls to see if `startI3LiveListener.py` is running. If not, it automatically
restarts the script.

Communication between the VM and the SNEWS server is handled by the third-party
library ``coincode`` library maintained by `Alec Habig <mailto:ahabig@umn.edu>`_.

SNEWS Monitoring Shifts
^^^^^^^^^^^^^^^^^^^^^^^

Experiments collaborating with SNEWS will shifts monitoring the SNEWS alert
servers. In IceCube this is the responsibility of members of the Supernova
Working Group, not winterovers or the monitoring shiftes. The following
outlines the steps for completing a monitoring shift, though some information
has been excluded in accordance with the SNEWS Collaboration's privacy policy.

First, a shifter must obtain login credentials for:

* ``sndaq`` virtual machine: contact the `IceCube Helpdesk * <mailto:help@icecube.wisc.edu>`_.
* Brookhaven National Lab (BNL) SSH Portal ``ssh.bnl.gov``: contact `Brett Viren <mailto:bv@bnl.gov>`_.
* INFN Bologna Portal, ``lnxbo.bo.infn.it``: contact `Pietro Antonioli <mailto:pietro.antonioli@bo.infn.it>`_.
* BNL and INFN machines hosting SNEWS alert servers: contact `Kate Scholberg <mailto:schol@phy.duke.edu>`_ or Alec Habig.
* `SNEWS Working Group Pages <https://snews.bnl.gov/wg/shift>`_: contact Kate Scholberg or Alec Habig.

The primary SNEWS server is hosted by BNL and a backup is hosted by INFN
Bologna. There is an online application to obtain credentials for BNL, but it
may take time ot process. Allot at least 3 weeks to obtain BNL credentials. To
obtain credentials for INFN Bologna, a paper form must be submitted with a
passport/ID card photocopy. Additionally, shifters must read and agree to
follow the SNEWS privacy policy.

SNEWS monitoring shifts last one week, during which a shifter must ping the
primary and backup SNEWS servers using an IceCube machine twice a day. To ping
the SNEWS servers from IceCube, log into the ``sndaq`` virtual machine.
Then execute the following command in the directory ``/home/sndaq/coinccode``:

.. code-block:: bash

  sndaq@sndaq:~$ ./shifter_cping

This is equivalent to running

.. code-block:: bash

  sndaq@sndaq:~$ ./cping all 0 0 0 6

where the arguments to ``cping`` are

.. code-block:: bash

  cping [server] [DDMMYY] [HHMMSS] [nanoseconds] [xperiment]

with IceCube being experiment **6**. After sending a ping from the IceCube
system, shifters must confirm it has been recieved by the SNEWS alert servers.
Log into the systems at BNL and INFN Bologna and then to the SNEWS alert
servers as follows:

.. code-block:: bash

  user@your_pc:~$ ssh user@ssh.bnl.gov
  user@ssh.bnl.gov:~$ ssh primary_server@primary_server_domain

  user@your_pc:~$ ssh user@lnxbo.bo.infn.it
  user@lnxbo:~$ ssh second_server@second_server_domain

Note that the names and domains of the machines hosting the SNEWS servers are
placeholders. A `SNEWS shifter manual <https://snews.bnl.gov/wg/shift>`_
available to the private SNEWS working group details how to check and record
that the SNEWS servers are running. Contact Kate Scholberg or Alec Habig to
obtain the credentials for the SNEWS alert servers and working group pages.

SNEWS 2.0
---------
