
# FAST-HEP Carpenter

Turns your trees into tables (ie. reads ROOT TTrees, writes summary Pandas DataFrames)

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]
[![Code style: black][black-badge]][black-link]

[![PyPI version][pypi-version]][pypi-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[actions-badge]: https://github.com/FAST-HEP/fasthep-carpenter/workflows/CI/badge.svg
[actions-link]: https://github.com/FAST-HEP/fasthep-carpenter/actions
[black-badge]:              https://img.shields.io/badge/code%20style-black-000000.svg
[black-link]:               https://github.com/psf/black
[pypi-link]:                https://pypi.org/project/fasthep-carpenter/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/fasthep-carpenter
[pypi-version]:             https://badge.fury.io/py/fasthep-carpenter.svg

[rtd-badge]:                https://readthedocs.org/projects/fasthep-carpenter/badge/?version=latest
[rtd-link]:                 https://fasthep-carpenter.readthedocs.io/en/latest/?badge=latest

## What is it?

fast-carpenter can:

* Be controlled using YAML-based config files
* Define new variables
* Cut out events or define phase-space "regions"
* Produce histograms stored as CSV files using multiple weighting schemes
* Make use of user-defined stages to manipulate the data

## Powered by

* uproot: to load ROOT Trees into memory as numpy arrays
* fast-flow: to manage the processing config files
* fast-curator: to orchestrate the lists of datasets to be processed

A tool from the [FAST-HEP](http://fast-hep.web.cern.ch/) collaboration.

## Installation and usage

Visit the `documentation <https://fasthep-carpenter.readthedocs.io/>`_ for full details.
