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

## Installation
Can be installed from pypi:
```bash
pip install --user fast-carpenter
```
or if you want to be able to edit code in this repo:
```
pip install --user -e git+https://gitlab.cern.ch/fast-hep/public/fast-carpenter.git#egg=fast_carpenter --src .
```
Note that to use this repository and the main `fast_carpenter` command, you normally shouldn't need to be able to edit this codebase;
in most instances the full analysis should be describable with just a config file, and in some cases custom, analysis-specific stages to create more tricky variables for example.

Also note that if you install this with pip, the main executable, `fast_carpenter`, will only be available everywhere if include the directory `~/.local/bin` in your `PATH` variable.

## Documentation
### Basic usage:
1. Build a description of the datasets you wish to process using the `fast_curator` command from the [fast-curator](://gitlab.cern.ch/fast-hep/public/fast-curator) package.
2. Write a description of what you want to do with your data (see documentation below).
3. Run things:
```bash
fast_carpenter datasets.yaml processing.yaml
```

You can use the built-in help as well for more info:
```
fast_carpenter --help
```

### Talk:
which partially act as documentation:
1. [IRIS-HEP 4th March 2019](https://indico.cern.ch/event/802182/contributions/3334624/)

## Example analysis
The [fast_cms_public_tutorial](https://gitlab.cern.ch/fast-hep/public/fast_cms_public_tutorial) shows an example analysis that uses this package.
