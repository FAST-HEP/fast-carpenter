# -*- coding: utf-8 -*-

"""Top-level package for fast-carpenter."""

__author__ = """Benjamin Krikler, and F.A.S.T"""
__email__ = 'fast-hep@cern.ch'
__version__ = '0.10.0'


from .define.variables import Define
from .define.systematics import SystematicWeights
from .selection.stage import CutFlow, SelectPhaseSpace
from .summary import BinnedDataframe, BuildAghast


__all__ = ["Define", "SystematicWeights", "CutFlow",
           "SelectPhaseSpace", "BinnedDataframe", "BuildAghast"]


known_stages = [Define, SystematicWeights, CutFlow,
                SelectPhaseSpace, BinnedDataframe, BuildAghast]
