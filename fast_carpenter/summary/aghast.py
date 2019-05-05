from __future__ import absolute_import
import os
from collections import defaultdict
import numpy as np
from . import binning_config as cfg
from . import binned_dataframe as binned_df
from .import_aghast import aghast


class Collector():
    def __init__(self, filename, axes, edges, by_dataset):
        self.filename = filename
        self.axes = axes
        self.edges = edges
        self.by_dataset = by_dataset

    def collect(self, dataset_readers_list):
        if len(dataset_readers_list) == 0:
            return None

        dataframe = binned_df.combined_dataframes(dataset_readers_list,
                                                  self.by_dataset,
                                                  binnings=self.edges)
        full_axes = complete_axes(self.axes, dataframe.index)
        dataframe.fillna(0, inplace=True)
        dataframe.to_csv(self.filename + ".csv")
        all_counters = convert_to_counters(dataframe)
        collection = aghast.Collection(all_counters, full_axes)
        collection.tofile(self.filename + ".ghst")


def convert_to_counters(df):
    counts = aghast.UnweightedCounts(aghast.InterpretedInlineFloat64Buffer(df[binned_df.count_label].values))
    counters = {binned_df.count_label: aghast.Histogram([aghast.Axis()], counts)}
    weight_labels = defaultdict(dict)
    for col in df.columns:
        if col == binned_df.count_label:
            continue
        label, sumtype = col.split(":")
        weight_labels[label][sumtype] = col
    for label, sums in weight_labels.items():
        sumw_col = sums[binned_df.weight_labels[0]]
        sumw = aghast.InterpretedInlineFloat64Buffer(df[sumw_col].values.astype(np.float64))

        sumw2 = None
        sumw2_col = sums.get(binned_df.weight_labels[1], None)
        if sumw2_col:
            sumw2 = aghast.InterpretedInlineFloat64Buffer(df[sumw2_col].values.astype(np.float64))
        counters[label] = aghast.Histogram([aghast.Axis()], aghast.WeightedCounts(sumw, sumw2))
    return counters


def complete_axes(axes, df_index):
    full_axes = []
    for dim in df_index.names:
        ax = axes.get(dim, None)
        if ax is None:
            values = df_index.unique(dim).astype(str)
            full_axes.append(aghast.Axis(aghast.CategoryBinning(values), title=dim))
        else:
            full_axes.append(ax)
    return full_axes


def _ovf_convention():
    return aghast.RealOverflow(loc_underflow=aghast.BinLocation.below1,
                               loc_overflow=aghast.BinLocation.above1)


def bin_one_dimension(low=None, high=None, nbins=None, edges=None,
                      **kwargs):
    # - bins: {nbins: 6 , low: 1  , high: 5 , overflow: True}
    # - bins: {edges: [0, 200., 900], overflow: True}
    if all([x is not None for x in (nbins, low, high)]):
        aghast_bins = aghast.RegularBinning(nbins,
                                            aghast.RealInterval(low, high),
                                            overflow=_ovf_convention())
    elif edges:
        # array are fixed to float type, to be consistent with the float-type underflow and overflow bins
        edges = np.array(edges, "f")
        aghast_bins = aghast.EdgesBinning(edges, overflow=_ovf_convention())
    else:
        return None
    return aghast_bins


class BuildAghast:
    """Builds an aghast histogram.

    Can be parametrized in the same way as
    :py:class:`fast_carpenter.BinnedDataframe` (and actually uses that stage
    behind the scenes) but additionally writes out a Ghast which can be
    reloaded with other ghast packages.

    .. seealso::

       * :py:class:`fast_carpenter.BinnedDataframe` for a version which only
         produces binned pandas dataframes.
       * The aghast main page: `<https://github.com/scikit-hep/aghast>`_.

    """

    def __init__(self, name, out_dir, binning, weights=None, dataset_col=True):
        self.name = name
        self.out_dir = out_dir
        self.builder = binned_df.BinnedDataframe(name + "_builder", out_dir,
                                                 binning=binning,
                                                 weights=weights,
                                                 dataset_col=dataset_col)
        ins, outs, bins = cfg.create_binning_list(self.name, binning, make_bins=bin_one_dimension)
        self.axes = {_out: aghast.Axis(_bins, title=_out, expression=_in) if _bins else None
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
        outfilename = os.path.join(self.out_dir, outfilename)
        edges = dict(zip(self.builder._out_bin_dims, self.builder._binnings))
        return Collector(outfilename, axes=self.axes,
                         edges=edges, by_dataset=self.by_dataset)

    def event(self, chunk):
        return self.builder.event(chunk)

    def merge(self, rhs):
        self.builder.merge(rhs.builder)
