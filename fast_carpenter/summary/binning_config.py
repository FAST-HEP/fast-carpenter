import six
import numpy as np


class BadBinnedDataframeConfig(Exception):
    pass


def create_binning_list(name, bin_list):
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
        _in, _out, _bins, _index = create_one_dimension(name, **cleaned_dimension_dict)
        ins.append(_in)
        outs.append(_out)
        indices.append(_index)
        binnings.append(_bins)
    return ins, outs, binnings


def create_one_dimension(stage_name, _in, _out=None, _bins=None, _index=None):
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
            # array are fixed to float type, to be consistent with the float-type underflow and overflow bins
            bin_obj = np.array(_bins["edges"], "f")
        else:
            msg = "{}: No way to infer binning edges for in={}"
            raise BadBinnedDataframeConfig(msg.format(stage_name, _in))
        if not _bins.get("disable_underflow", False):
            bin_obj = np.insert(bin_obj, 0, float("-inf"))
        if not _bins.get("disable_overflow", False):
            bin_obj = np.append(bin_obj, float("inf"))
    else:
        msg = "{}: bins is neither None nor a dictionary for in={}"
        raise BadBinnedDataframeConfig(msg.format(stage_name, _in))

    return (str(_in), str(_out), bin_obj, _index)


def create_weights(stage_name, weights):
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
