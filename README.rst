

.. image:: https://img.shields.io/pypi/v/fast-carpenter.svg
   :target: https://pypi.org/project/fast-carpenter/
   :alt: pypi package


.. image:: https://travis-ci.com/FAST-HEP/fast-carpenter.svg?branch=master
   :target: https://travis-ci.com/FAST-HEP/fast-carpenter
   :alt: pipeline status


.. image:: https://codecov.io/gh/FAST-HEP/fast-carpenter/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/FAST-HEP/fast-carpenter
   :alt: coverage report


.. image:: https://readthedocs.org/projects/fast-carpenter/badge/?version=latest
   :target: https://fast-carpenter.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status


.. image:: https://badges.gitter.im/FAST-HEP/community.svg
   :target: https://gitter.im/FAST-HEP/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge
   :alt: Gitter

.. image:: https://zenodo.org/badge/187055992.svg
   :target: https://zenodo.org/badge/latestdoi/187055992

.. image:: https://raw.githubusercontent.com/FAST-HEP/logos-etc/master/fast-carpenter-black.png
   :target: https://github.com/fast-hep/fast-carpenter
   :alt: fast-carpenter

Turns your trees into tables (ie. reads ROOT TTrees, writes summary Pandas DataFrames)


fast-carpenter can:


* Be controlled using YAML-based config files
* Define new variables
* Cut out events or define phase-space "regions"
* Produce histograms stored as CSV files using multiple weighting schemes
* Make use of user-defined stages to manipulate the data


Powered by:


* AlphaTwirl (presently): to run the dataset splitting
* Atuproot: to adapt AlphaTwirl to use uproot
* uproot: to load ROOT Trees into memory as numpy arrays
* fast-flow: to manage the processing config files
* fast-curator: to orchestrate the lists of datasets to be processed
* Espresso: to keep the developer(s) writing code


A tool from the Faster Analysis Software Taskforce: http://fast-hep.web.cern.ch/

Installation and usage
----------------------

Visit the `documentation <https://fast-carpenter.readthedocs.io/>`_ for full details.
