"""
"""
import six
import pandas as pd
import numpy as np
import os


class BadBinnedDataframeConfig(Exception):
    pass


class Collector():
    def __init__(self, filename):
        self.filename = filename

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
                if final_df is None:
                    final_df = df
                    continue
                final_df.add(df, fill_value=0)

        return final_df


class BinnedDataframe():

    def __init__(self, name, out_dir, binning, weights=None):
        self.name = name
        self.out_dir = out_dir
        ins, outs, binnings = _create_binning_list(self.name, binning)
        self._bin_dims = ins
        self._out_bin_dims = outs
        self._binnings = binnings
        self._weights = _create_weights(self.name, weights)

        self._all_inputs = self._bin_dims
        if self._weights:
            self._all_inputs += self._weights.values()

    def collector(self):
        outfilename = "tbl_"
        outfilename += ".".join(self._out_bin_dims)
        outfilename += "--" + ".".join(self._weights.keys())
        outfilename += ".csv"
        outfilename = os.path.join(self.out_dir, outfilename)
        return Collector(outfilename)

    def begin(self, event):
        self.contents = None

    #def end(self, event):
    #    pass

    # def merge(self, event):
    #     pass

    def event(self, chunk):
        data = chunk.tree.pandas.df(self._all_inputs)
        weight = self._weights.values()[0]
        binned_values = _bin_values(data, self._bin_dims, self._out_bin_dims, 
                                    self._binnings, weight)
        if not self.contents:
            self.contents = binned_values
        else:
            self.contents = self.contents.add(binned_values, fill_value=0)
        return True


def _bin_values(data, bin_dim_names, out_dim_names, binnings, weight_dims):
    # TODO: Optimise the for-loop to run pandas.cut
    # TODO: Optimise out deduction of input and output binning and weight names

    final_bin_dims = []
    for dimension, binning in zip(bin_dim_names, binnings):
        if not binning:
            final_bin_dims.append(dimension)
            continue
        out_dimension = dimension + "_bins"
        data[out_dimension] = pd.cut(data[dimension], binning)
        final_bin_dims.append(out_dimension)

    if weight_dims:
        weight_sq_dims = weight_dims + "_squared"
        data[weight_sq_dims] = data[weight_dims] * data[weight_dims]

    bins = data.groupby(final_bin_dims)
    counts = bins.size()

    if weight_dims:
        sums = bins[weight_dims].sum()
        sum_sqs = bins[weight_sq_dims].sum()
    else:
        sums = counts
        sum_sqs = counts

    histogram = pd.concat([counts, sums, sum_sqs], axis="columns")
    histogram.columns=["count", "contents", "variance"]
    histogram.index.set_names(out_dim_names, inplace=True)
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


def _create_one_dimension(stage_name, _in, _out, _bins=None, _index=None):
    if not isinstance(_in, six.string_types):
        msg = "{}: binning dictionary contains non-string value for 'in'"
        raise BadBinnedDataframeConfig(msg.format(stage_name))
    if not isinstance(_out, six.string_types):
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
        return None
    if isinstance(weights, list):
        weights = {str(w): w for w in weights}
    elif isinstance(weights, dict):
        weights = {k: w for k, w in weights.items()}
    else:
        # else we've got a single, scalar value
        weights = {"weighted": weights}
    if len(weights) > 1:
        raise NotImplementedError("Multiply weighted binned dataframes aren't yet implemented I'm afraid...")
    return weights
