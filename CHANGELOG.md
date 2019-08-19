# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed

### Removed

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
