fast-carpenter
=============
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
git clone ssh://git@gitlab.cern.ch:7999/fast-hep/public/fast-carpenter.git 
cd fast-carpenter
pip install --user -e .
```
Note that to use this repository and the main `fast_carpenter` command, you normally shouldn't need to be able to edit this codebase;
in most instances the full analysis should be describable with just a config file, and in some cases custom, analysis-specific stages for instance to create more tricky variables.

## Documentation
### Basic usage:
1. Build a description of the datasets you wish to process using the `fast_curator` command from the [fast-curator](://gitlab.cern.ch/fast-hep/public/fast-curator) package.
2. Write a description of what you want to do with your data (see documentation below).
3. Run things:
```bash
fast_carpenter datasets.yaml processing.yaml
```

### The processing config file
... is on its way...

## Example analysis
...is also on its way...
