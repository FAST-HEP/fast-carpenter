"""
"""
import six
import pandas as pd
import numpy as np
import os


class BadBinnedDataframeConfig(Exception):
    pass


class Collector():
    def __init__(self, filename, dataset_col):
        self.filename = filename
        self.dataset_col = dataset_col

    def collect(self, dataset_readers_list):
        if len(dataset_readers_list) == 0:
            return None

        dataset_readers_list = [(d, r) for d, r in dataset_readers_list if r]
        if len(dataset_readers_list) == 0:
            return None

        output = self._merge_dataframes(dataset_readers_list)
        output.to_csv(self.filename)

    def _merge_dataframes(self, dataset_readers_list):
        final_df = None
        for dataset, readers in dataset_readers_list:
            for reader in readers:
                df = reader.contents
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
        ins, outs, binnings = _create_binning_list(self.name, binning)
        self._bin_dims = ins
        self._out_bin_dims = outs
        self._binnings = binnings
        self._dataset_col = dataset_col
        self._weights = _create_weights(self.name, weights)

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

    def begin(self, event):
        self.contents = None

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
        self.contents = binned_values

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


def _create_binning_list(name, bin_list):
    if not isinstance(bin_list, list):
        raise BadBinnedDataframeConfig("binning section for stage '{}' not a list".format(name))
    ins = []
    outs = []
    binnings = []
    indices = []
    for i, one_bin_dimension in enumerate(bin_list):
        if not isinstance(one_bin_dimension, dict):
            raise BadBinnedDataframeConfig("binning item no. {} is not a dictionary".format(i))
        cleaned_dimension_dict = {"_" + k: v for k, v in one_bin_dimension.items()}
        _in, _out, _bins, _index = _create_one_dimension(name, **cleaned_dimension_dict)
        ins.append(_in)
        outs.append(_out)
        indices.append(_index)
        binnings.append(_bins)
    return ins, outs, binnings


def _create_one_dimension(stage_name, _in, _out=None, _bins=None, _index=None):
    if not isinstance(_in, six.string_types):
        msg = "{}: binning dictionary contains non-string value for 'in'"
        raise BadBinnedDataframeConfig(msg.format(stage_name))
    if _out is None:
        _out = _in
    elif not isinstance(_out, six.string_types):
        msg = "{}: binning dictionary contains non-string value for 'out'"
        raise BadBinnedDataframeConfig(msg.format(stage_name))
    if _index and not isinstance(_index, six.string_types):
        msg = "{}: binning dictionary contains non-string and non-integer value for 'index'"
        raise BadBinnedDataframeConfig(msg.format(stage_name))

    if _bins is None:
        bin_obj = None
    elif isinstance(_bins, dict):
        # - bins: {nbins: 6 , low: 1  , high: 5 , overflow: True}
        # - bins: {edges: [0, 200., 900], overflow: True}
        if "nbins" in _bins and "low" in _bins and "high" in _bins:
            low = _bins["low"]
            high = _bins["high"]
            nbins = _bins["nbins"]
            bin_obj = np.linspace(low, high, nbins + 1)
        elif "edges" in _bins:
            bin_obj = np.array(_bins["edges"])
        else:
            msg = "{}: No way to infer binning edges for in={}"
            raise BadBinnedDataframeConfig(msg.format(stage_name, _in))
        bin_obj = np.insert(bin_obj, 0, float("-inf"))
        bin_obj = np.append(bin_obj, float("inf"))
    else:
        msg = "{}: bins is neither None nor a dictionary for in={}"
        raise BadBinnedDataframeConfig(msg.format(stage_name, _in))

    return (str(_in), str(_out), bin_obj, _index)


def _create_weights(stage_name, weights):
    if weights is None:
        return {}
    if isinstance(weights, list):
        weights = {str(w): w for w in weights}
    elif isinstance(weights, dict):
        pass
    else:
        # else we've got a single, scalar value
        weights = {weights: weights}
    return weights
