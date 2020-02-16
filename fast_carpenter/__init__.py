# -*- coding: utf-8 -*-

"""Top-level package for fast-carpenter."""

__author__ = """Benjamin Krikler, and F.A.S.T"""
__email__ = 'fast-hep@cern.ch'


from .define.variables import Define
from .define.systematics import SystematicWeights
from .selection.stage import CutFlow, SelectPhaseSpace
from .summary import BinnedDataframe, BuildAghast, EventByEventDataframe
from .version import __version__, version_info


__all__ = ["Define", "SystematicWeights", "CutFlow",
           "SelectPhaseSpace", "BinnedDataframe", "BuildAghast",
           "__version__", "version_info"]


known_stages = [Define, SystematicWeights, CutFlow,
                SelectPhaseSpace, BinnedDataframe, BuildAghast,
                EventByEventDataframe]
