[![pypi package](https://img.shields.io/pypi/v/fast-carpenter.svg)](https://pypi.org/project/fast-carpenter/)
[![pipeline status](https://gitlab.cern.ch/fast-hep/public/fast-carpenter/badges/master/pipeline.svg)](https://gitlab.cern.ch/fast-hep/public/fast-carpenter/commits/master)
[![coverage report](https://gitlab.cern.ch/fast-hep/public/fast-carpenter/badges/master/coverage.svg)](https://gitlab.cern.ch/fast-hep/public/fast-carpenter/commits/master)
[![Documentation Status](https://readthedocs.org/projects/fast-carpenter/badge/?version=latest)](https://fast-carpenter.readthedocs.io/en/latest/?badge=latest)


# fast-carpenter
Turns your trees into tables (ie. reads ROOT TTrees, writes summary Pandas DataFrames)

fast-carpenter can:
- Be controlled using YAML-based config files
- Define new variables
- Cut out events or define phase-space "regions"
- Produce histograms stored as CSV files using multiple weighting schemes
- Make use of user-defined stages to manipulate the data

Powered by:
- AlphaTwirl (presently): to run the dataset splitting
- Atuproot: to adapt AlphaTwirl to use uproot
- uproot: to load ROOT Trees into memory as numpy arrays
- fast-flow: to manage the processing config files
- fast-curator: to orchestrate the lists of datasets to be processed
- coffee: to help the developer(s) write code

## Installation and usage
Visit the [documentation](https://fast-carpenter.readthedocs.io/en/) for full details.
