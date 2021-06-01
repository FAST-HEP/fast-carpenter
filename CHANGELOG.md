# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [unreleased]
### Fixed
- Remove numba dependency, PR #139 [@benkrikler](https://github.com/benkrikler)

## [0.19.1] - 2020-10-13
### Fixed
- Fix for uproot 3.13.0, PR #138 [@benkrikler](https://github.com/benkrikler)
- Fix call to pandas sys-info function for book-keeping, PR #130 [@bundocka](https://github.com/bundocka)

## [0.19.0] - 2020-08-19
### Added
- Put a tarball with book-keeping data in output directory, PR #131 [@BenKrikler](https://github.com/benkrikler)
- Add Dask and Parsl backends from Coffea and support execution configuration via YAML, PR #129 [@BenKrikler](https://github.com/benkrikler) and [@asnaylor](https://github.com/asnaylor)

### Fixed
- Pin alphatwirl explicitly to 0.25.5, PR #130

### Removed
- Removed help module from carpenter and moved to fast-flow, PR #131 [@BenKrikler](https://github.com/benkrikler)

## [0.18.2] - 2020-07-01
### Added
- Add option to BinnedDataframe to apply weights to data like MC, PR #127 [@BenKrikler](https://github.com/benkrikler)

## [0.18.1] - 2020-06-17
### Fixed
- Fix binned_dataframe.explode for object-level and non-initial data chunks, PR #125

## [0.18.0] - 2020-06-17
### Added
- Add broadcasting between variables of different jaggedness in expressions, PR #122 [@BenKrikler](https://github.com/benkrikler)

### Removed
- Testing against Python <= 3.5, PR #124

### Fixed
- Fix handling of empty data chunks in BinnedDataframe stage, PR #124 [@BenKrikler](https://github.com/benkrikler)

## [0.17.5] - 2020-04-03
### Added
- Add `observed` option to BinnedDataframe for speed boost with many bins, PR #118 [@BenKrikler](https://github.com/benkrikler)

### Changed
- Pin the version for the Mantichora package that AlphaTwirl depends on

## [0.17.4] - 2020-03-12
### Changed
- `pad_missing` was replacing bin contents when set to True, PR #116 [@BenKrikler](https://github.com/benkrikler)

## [0.17.3] - 2020-02-27
### Changed
- Allow SystematicWeights stage to be used twice, PR #115 [@BenKrikler](https://github.com/benkrikler)

## [0.17.2] - 2020-02-25
### Changed
- Upgrade atsge to v0.2.1

## [0.17.1] - 2020-02-16
### Changed
- Updated version in setup.py to mark as a new version
- Upgrade atsge to v0.2.0

## [0.17.0] - 2020-02-16
### Add
- New stage: Event-by-Event dataframes (like a skim), PR #108 [@davignon](https://github.com/davignon/)

## [0.16.1] - 2020-02-16
### Fixed
- Unit test that was broken by Pandas >1.0.0
- Bug in explode function when an dimension contains strings, issue #109, PR #110 [@BenKrikler](https://github.com/benkrikler)

## [0.16.0] - 2019-11-1
### Added
- An API for different "backends" which run the actual data processing
- Add a backed for Coffea with local-multiprocessing executor. PR #102 [@BenKrikler](https://github.com/benkrikler)

### Changed
- Refactor AlphaTwirl code to use the "backend" API. PR #101 [@BenKrikler](https://github.com/benkrikler)

## [0.15.1] - 2019-11-1
### Changed
- Added support for variables with multiple dots in the name (nested branches). Issue #95, PR #97 [@kreczko](https://github.com/kreczko)
- Fix JaggedNth to work with arbitrary depth jagged arrays, Issue #87, PR #99 [@benkrikler](https://github.com/benkrikler)
- Add protection against multiple dimensions using the same output name in a BinnedDataframe stage, Issue #92, PR #100 [@benkrikler](https://github.com/benkrikler)

## [0.15.0] - 2019-10-27
### Added
- Existing collectors may now return their results within python, in addition to writing them to disk PR #90 [@lgray](https://github.com/lgray). 
   - This behavior can, at present, only be controlled within python, and is meant for exposing certain aspects of FAST-carpenter plumbing to [coffea](https://github.com/CoffeaTeam/coffea).

### Changed
- Fix bug in BinnedDataframe stage, issue #89, PR #93 [@benkrikler](https://github.com/benkrikler)
- Pin atuproot to v0.1.13, PR #91
- Tidy the print out at the end of processing, PR #94.

## [0.14.3] - 2019-10-07
### Added
- Give fast-flow the "fast_carpenter" backend, issue #84, PR #85 [@lorenafreitas](https://github.com/lorenafreitas)

## [0.14.2] - 2019-10-06
### Added
- JaggedNth supports negative indexing, PR #81 [@pmk21](https://github.com/pmk21!)

## [0.14.1] - 2019-10-04
### Added
- Added version flag to CLI, PR #79. [@maikefischer](github.com/maikefischer)
- Prohibit ncores < 1, PR #76 [@annakau](https://github.com/annakau)

### Changed
- Binned dataframes can now be produced from ND jagged arrays

## [0.14.0] - 2019-10-03
### Added
- Support for ND jagged array in expressions, PR #73
- Automatic conversion of ObjectArrays from uproot to JaggedArrays, PR #73

## [0.13.4] - 2019-09-21
### Changed
- Fixed interpretation of user-defined variables for uproot, issue #67, PR #71 [@benkrikler](https://github.com/benkrikler)

## [0.13.3] - 2019-09-12
### Changed
- Add changes to support uproot 3.9.1 and greater, issue #68 [@benkrikler](https://github.com/benkrikler)

## [0.13.2] - 2019-08-19
### Changed
- Protect against overwriting branches and add tests, pull request #66 [@benkrikler](https://github.com/benkrikler)

## [0.13.1] - 2019-08-05
### Added
- Adds support for masking variables in their definition, issue #59 [@benkrikler](https://github.com/benkrikler)
- Adds several constants for variable expressions, issue #21 [@benkrikler](https://github.com/benkrikler)
- Added this changelog [@benkrikler](https://github.com/benkrikler)

### Changed
- Use `pandas.groupby(..).counts()` to make binned dataframes, issue #51 [@benkrikler](https://github.com/benkrikler)

### Removed

## [0.13.0] - 2019-07-28
### Added
- Add support for multiple output file types for binned dataframes, issue #57 [@asnaylor](https://github.com/asnaylor)

### Changed
- Fix issue with binned dataframe expressions and multiple similar branch names, issue #60 [@benkrikler](https://github.com/benkrikler)
- Enable multiple cut-flow stages and make the input chunk.tree have consistent array and arrays methods, issue #61 [@benkrikler](https://github.com/benkrikler)

### Removed

## [0.12.0] - 2019-05-28
