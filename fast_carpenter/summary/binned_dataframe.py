"""
"""
import pandas as pd
import os
from . import binning_config as cfg


class Collector():
    def __init__(self, filename, dataset_col):
        self.filename = filename
        self.dataset_col = dataset_col

    def collect(self, dataset_readers_list):
        if len(dataset_readers_list) == 0:
            return None

        output = self._prepare_output(dataset_readers_list)
        output.to_csv(self.filename)

    def _prepare_output(self, dataset_readers_list):
        dataset_readers_list = [(d, r) for d, r in dataset_readers_list if r]
        if len(dataset_readers_list) == 0:
            return None

        return self._merge_dataframes(dataset_readers_list)

    def _merge_dataframes(self, dataset_readers_list):
        final_df = None
        for dataset, readers in dataset_readers_list:
            for reader in readers:
                df = reader.contents
                if df is None:
                    continue
                if self.dataset_col:
                    df = pd.concat([df], keys=[dataset], names=['dataset'])
                if final_df is None:
                    final_df = df
                    continue
                final_df = final_df.add(df, fill_value=0)

        return final_df


class BinnedDataframe():

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


def _bin_values(data, dimensions, binnings, weights, out_dimensions=None, out_weights=None):
    if not out_dimensions:
        out_dimensions = dimensions
    if not out_weights:
        out_weights = weights

    final_bin_dims = []
    for dimension, binning in zip(dimensions, binnings):
        if binning is None:
            final_bin_dims.append(dimension)
            continue
        out_dimension = dimension + "_bins"
        data[out_dimension] = pd.cut(data[dimension], binning)
        final_bin_dims.append(out_dimension)

    if weights:
        weight_sq_dims = [w + "_squared" for w in weights]
        data[weight_sq_dims] = data[weights] ** 2

    bins = data.groupby(final_bin_dims)
    counts = bins.size()

    if weights:
        sums = bins[weights].sum()
        sum_sqs = bins[weight_sq_dims].sum()
        histogram = pd.concat([counts, sums, sum_sqs], axis="columns")
        histogram.columns = _make_column_labels(out_weights)
    else:
        histogram = counts.to_frame(_count_label)

    histogram.index.set_names(out_dimensions, inplace=True)
    return histogram
