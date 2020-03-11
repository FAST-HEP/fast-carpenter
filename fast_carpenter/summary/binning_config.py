import six
import numpy as np
import pandas as pd


class BadBinnedDataframeConfig(Exception):
    pass


def create_binning_list(name, bin_list, make_bins=None):
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
        _in, _out, _bins, _index = create_one_dimension(name,
                                                        make_bins=make_bins,
                                                        **cleaned_dimension_dict)
        ins.append(_in)
        outs.append(_out)
        indices.append(_index)
        binnings.append(_bins)
    if len(set(outs)) != len(outs):
        msg = "{}: some binning dimensions repeat `out` names"
        raise BadBinnedDataframeConfig(msg.format(name))
    return ins, outs, binnings


def create_one_dimension(stage_name, _in, _out=None, _bins=None, _index=None, make_bins=None):
    if not make_bins:
        make_bins = bin_one_dimension
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
        bin_obj = make_bins(**_bins)
        if bin_obj is None:
            msg = "{}: No way to infer binning edges for in={}"
            raise BadBinnedDataframeConfig(msg.format(stage_name, _in))
    else:
        msg = "{}: bins is neither None nor a dictionary for in={}"
        raise BadBinnedDataframeConfig(msg.format(stage_name, _in))

    return (str(_in), str(_out), bin_obj, _index)


def bin_one_dimension(low=None, high=None, nbins=None, edges=None,
                      overflow=True, underflow=True):
    # - bins: {nbins: 6 , low: 1  , high: 5 , overflow: True}
    # - bins: {edges: [0, 200., 900], overflow: True}
    if all([x is not None for x in (nbins, low, high)]):
        low, high, nbins = (pd.eval(low, engine='numexpr'),
                            pd.eval(high, engine='numexpr'), pd.eval(nbins, engine='numexpr'))
        bin_obj = np.linspace(low, high, nbins + 1)
    elif edges:
        # array are fixed to float type, to be consistent with the float-type underflow and overflow bins
        bin_obj = np.array(edges, "f")
    else:
        return None
    if underflow:
        bin_obj = np.insert(bin_obj, 0, float("-inf"))
    if overflow:
        bin_obj = np.append(bin_obj, float("inf"))
    bin_obj = pd.IntervalIndex.from_breaks(bin_obj, closed="left")
    return bin_obj


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


def create_file_format(stage_name, file_format):
    if file_format is None:
        return [{'extension': '.csv', 'float_format': '%.17g'}]
    if isinstance(file_format, list):
        file_format = [file_dict
                       if isinstance(file_dict, dict)
                       else {'extension': file_dict}
                       for file_dict in file_format]
    elif isinstance(file_format, dict):
        file_format = [file_format]
    else:
        # else we've got a single, scalar value
        file_format = [{'extension': file_format}]
    return file_format
