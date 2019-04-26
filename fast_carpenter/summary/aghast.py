import os
import pandas as pd
from aghast import Histogram, UnweightedCounts, WeightedCounts, Axis
from aghast import InterpretedInlineBuffer, RealInterval, CategoryBinning
from aghast import RegularBinning, IntegerBinning, EdgesBinning, IrregularBinning
from . import binning_config as cfg


class Collector():
    def __init__(self, filename, dataset_col):
        self.filename = filename
        self.dataset_col = dataset_col

    def collect(self, dataset_readers_list):
        if len(dataset_readers_list) == 0:
            return None

        # output = self._prepare_output(dataset_readers_list)
        # output.to_csv(self.filename)


class BuildAghast:

    def __init__(self, name, out_dir, binning, weights=None, dataset_col=False):
        self.name = name
        self.out_dir = out_dir
        ins, outs, binnings = cfg.create_binning_list(self.name, binning)
        self._bin_dims = ins
        self._out_bin_dims = outs
        self._binnings = binnings
        self._dataset_col = dataset_col
        self._weights = cfg.create_weights(self.name, weights)
        self.contents = None

    def collector(self):
        outfilename = "tbl_"
        if self._dataset_col:
            outfilename += "dataset."
        outfilename += ".".join(self._out_bin_dims)
        if self._weights:
            outfilename += "--" + ".".join(self._weights.keys())
        outfilename += ".csv"
        outfilename = os.path.join(self.out_dir, outfilename)
        return Collector(outfilename, self._dataset_col)

    def event(self, chunk):
        if chunk.config.dataset.eventtype == "mc":
            weights = list(self._weights.values())
            all_inputs = self._bin_dims + weights
        else:
            weights = None
            all_inputs = self._bin_dims

        data = chunk.tree.pandas.df(all_inputs)

        binned_values = _bin_values(data, dimensions=self._bin_dims,
                                    binnings=self._binnings,
                                    weights=weights,
                                    out_weights=self._weights.keys(),
                                    out_dimensions=self._out_bin_dims)
        if self.contents is None:
            self.contents = binned_values
        else:
            self.contents = self.contents.add(binned_values, fill_value=0)

        return True

    def merge(self, rhs):
        if rhs.contents is None or len(rhs.contents) == 0:
            return
        if self.contents is None:
            self.contents = rhs.contents
            return
        self.contents = self.contents.add(rhs.contents, fill_value=0)


_count_label = "n"
_weight_labels = ["sumw", "sumw2"]


def _make_column_labels(weights):
    weight_labels = [w + ":" + l for l in _weight_labels for w in weights]
    return [_count_label] + weight_labels


def _make_axes(index, are_intervals):
    if isinstance(index, pd.MultiIndex):
        levels = index.levels
    else:
        levels = [index]
        
    axes = []
    for level, is_interval in zip(levels, are_intervals):
        if is_interval:
            print (level.name, " ===== INTERVAL-INDEX")
        elif level.dtype == int:
            print (level.name, " ===== INT")
            mini = level.min()
            maxi = level.max()
            binning = IntegerBinning(min=mini, max=maxi)
            axes.append(Axis(binning))
        elif isinstance(level, pd.CategoricalIndex):
            binning = CategoryBinning(level.unique())
            axes.append(Axis(binning))
            print (level.name, " ===== CATEGORY")
        elif isinstance(level, pd.IntervalIndex):
                print (level.name, " ===== INTERVAL-INDEX")
        print(level, level.dtype)
        print("is int: ", level.dtype == int)
        print("\n")
    return axes


def _bin_values(data, dimensions, binnings, weights, out_dimensions=None, out_weights=None):
    if not out_dimensions:
        out_dimensions = dimensions
    if not out_weights:
        out_weights = weights

    final_bin_dims = []
    are_intervals = []
    for dimension, binning in zip(dimensions, binnings):
        if binning is None:
            final_bin_dims.append(dimension)
            are_intervals.append(False)
            continue
        out_dimension = dimension + "_bins"
        data[out_dimension] = pd.cut(data[dimension], binning, right=False)
        print("\n### dtype for", out_dimension, data[out_dimension].dtype)
        final_bin_dims.append(out_dimension)
        are_intervals.append(True)

    if weights:
        weight_sq_dims = [w + "_squared" for w in weights]
        data[weight_sq_dims] = data[weights] ** 2

    bins = data.groupby(final_bin_dims)
    counts = bins.size()
    axes = _make_axes(counts.index, are_intervals)

    if weights:
        sums = bins[weights].sum()
        sum_sqs = bins[weight_sq_dims].sum()
        histogram = pd.concat([counts, sums, sum_sqs], axis="columns")
        histogram.columns = _make_column_labels(out_weights)
    else:
        histogram = counts.to_frame(_count_label)

    histogram.index.set_names(out_dimensions, inplace=True)

    hist = Histogram(axis=axes, counts=counts)
    return hist
