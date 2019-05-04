import os
import pandas as pd
import numpy as np
from aghast import Histogram, UnweightedCounts, WeightedCounts, Axis, BinLocation
from aghast import InterpretedInlineInt64Buffer, RealInterval, CategoryBinning, RealOverflow
from aghast import RegularBinning, IntegerBinning, EdgesBinning, IrregularBinning
from . import binning_config as cfg
from . import binned_dataframe as binned_df
from collections import namedtuple


class Collector():
    def __init__(self, filename, axes, edges, by_dataset):
        self.filename = filename
        self.axes = axes
        self.edges = edges
        self.by_dataset = by_dataset

    def collect(self, dataset_readers_list):
        if len(dataset_readers_list) == 0:
            return None

        dataframe = binned_df.combined_dataframes(dataset_readers_list, self.by_dataset, binnings=self.edges)
        full_axes = complete_axes(self.axes, dataframe.index)
        counts = UnweightedCounts(InterpretedInlineInt64Buffer(dataframe[binned_df.count_label].values))
        hist = Histogram(full_axes, counts)
        hist.tofile(self.filename)


def complete_axes(axes, df_index):
    full_axes = []
    for dim in df_index.names:
        ax = axes.get(dim, None)
        if ax is None:
            values = df_index.unique(dim).astype(str)
            full_axes.append(Axis(CategoryBinning(values), title=dim))
        else:
            full_axes.append(ax)
    return full_axes


_ovf_convention = lambda: RealOverflow(loc_underflow=BinLocation.below1,
                                       loc_overflow=BinLocation.above1)


def bin_one_dimension(low=None, high=None, nbins=None, edges=None,
                      **kwargs):
    # - bins: {nbins: 6 , low: 1  , high: 5 , overflow: True}
    # - bins: {edges: [0, 200., 900], overflow: True}
    if all([x is not None for x in (nbins, low, high)]):
        aghast_bins = RegularBinning(nbins,
                                     RealInterval(low, high),
                                     overflow=_ovf_convention())
    elif edges:
        # array are fixed to float type, to be consistent with the float-type underflow and overflow bins
        edges = np.array(edges, "f")
        aghast_bins = EdgesBinning(edges, overflow=_ovf_convention())
    else:
        return None
    return aghast_bins


class BuildAghast:

    def __init__(self, name, out_dir, binning, weights=None, dataset_col=True):
        self.name = name
        self.out_dir = out_dir
        self.builder = binned_df.BinnedDataframe(name + "_builder", out_dir,
                                                 binning=binning,
                                                 weights=weights,
                                                 dataset_col=dataset_col)
        ins, outs, bins = cfg.create_binning_list(self.name, binning, make_bins=bin_one_dimension)
        self.axes = {_out: Axis(_bins, title=_out, expression=_in) if _bins else None
                     for _in, _out, _bins in zip(ins, outs, bins)}
        self.by_dataset = dataset_col

    @property
    def contents(self):
        return self.builder.contents

    def collector(self):
        outfilename = "tbl_"
        if self.by_dataset:
            outfilename += "dataset."
        outfilename += ".".join(self.axes.keys())
        outfilename += "--" + self.name
        outfilename += ".ghst"
        outfilename = os.path.join(self.out_dir, outfilename)
        edges = dict(zip(self.builder._out_bin_dims, self.builder._binnings))
        return Collector(outfilename, axes=self.axes,
                         edges=edges, by_dataset=self.by_dataset)

    def event(self, chunk):
        return self.builder.event(chunk)

    def merge(self, rhs):
        self.builder.merge(rhs.builder)
