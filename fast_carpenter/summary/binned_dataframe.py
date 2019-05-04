"""
Summarize the data by producing binned and possibly weighted counts of the data.
"""
import os
import pandas as pd
from . import binning_config as cfg


class Collector():
    def __init__(self, filename, dataset_col, binnings):
        self.filename = filename
        self.dataset_col = dataset_col
        self.binnings = binnings

    def collect(self, dataset_readers_list):
        if len(dataset_readers_list) == 0:
            return

        output = self._prepare_output(dataset_readers_list)
        output.to_csv(self.filename, float_format="%.17g")

    def _prepare_output(self, dataset_readers_list):
        return combined_dataframes(dataset_readers_list,
                                   self.dataset_col,
                                   binnings=self.binnings)


def combined_dataframes(dataset_readers_list, dataset_col, binnings=None):
    dataset_readers_list = [(d, [r.contents for r in readers])
                            for d, readers in dataset_readers_list if readers]
    if not dataset_readers_list:
        return None

    if dataset_col:
        output = _merge_dataframes(dataset_readers_list)
    else:
        output = _sum_dataframes(dataset_readers_list)
    if binnings:
        output = densify_dataframe(output, binnings)
    return output


def _merge_dataframes(dataset_readers_list):
    all_dfs = []
    keys = []
    for dataset, readers in dataset_readers_list:
        dataset_df = readers[0]
        for df in readers[1:]:
            if df is None:
                continue
            dataset_df = dataset_df.add(df, fill_value=0.)
        all_dfs.append(dataset_df)
        keys.append(dataset)
    final_df = pd.concat(all_dfs, keys=keys, names=['dataset'], sort=True)
    return final_df


def _sum_dataframes(dataset_readers_list):
    final_df = None
    for _, readers in dataset_readers_list:
        for df in readers:
            if df is None:
                continue
            if final_df is None:
                final_df = df
                continue
            final_df = final_df.add(df, fill_value=0.)
    return final_df


def densify_dataframe(in_df, binnings):
    in_index = in_df.index
    index_values = []
    for dim in in_index.names:
        bins = binnings.get(dim, None)
        if bins is None:
            index_values.append(in_index.unique(dim))
            continue
        index_values.append(pd.IntervalIndex.from_breaks(bins, closed="left"))
    out_index = pd.MultiIndex.from_product(index_values, names=in_index.names)
    out_df = in_df.reindex(index=out_index, copy=False)
    return out_df


class BinnedDataframe():
    """Produces a binned dataframe (a multi-dimensional histogram).

    def __init__(self, name, out_dir, binning, weights=None, dataset_col=False):

    Parameters:
      binning (list[dict]): A list of dictionaries describing the variables to
        bin on, and how they should be binned.  Each of these dictionaries can
        contain the following:

        +-----------+----------------+--------------------------------------------------------------+
        | Parameter | Default        | Description                                                  |
        +===========+================+==============================================================+
        | ``in``    |                | The name of the attribute on the event to use.               |
        +-----------+----------------+--------------------------------------------------------------+
        | ``out``   | same as ``in`` | The name of the column to be filled in the output dataframe. |
        +-----------+----------------+--------------------------------------------------------------+
        | ``bins``  | ``None``       | | Must be either ``None`` or a dictionary.  If a dictionary, |
        |           |                |   it must contain one of the follow sets of                  |
        |           |                | | key-value pairs:                                           |
        |           |                | |   1. ``nbins``, ``low``, ``high``: which are used to       |
        |           |                |     produce a list of bin edges equivalent to:               |
        |           |                | |      ``numpy.linspace(low, high, nbins + 1)``              |
        |           |                | |   2. ``edges``: which is treated as the list of bin        |
        |           |                |     edges directly.                                          |
        |           |                | | If set to ``None``, then the input variable is assumed     |
        |           |                |   to already be categorical (ie. binned or discrete)         |
        +-----------+----------------+--------------------------------------------------------------+

      weights (str or list[str], dict[str, str]): How to weight events in the
        output table.  Must be either a single variable, a list of
        variables, or a dictionary where the values are variables in the data and
        keys are the column names that these weights should be called in the
        output tables.
      dataset_col (bool): adds an extra binning column with the name for each dataset.
      pad_missing (bool): If ``False``, any bins that don't contain data are
        excluded from the stored dataframe.  Leaving this ``False`` can save
        some disk-space and improve processing time, particularly if the bins are
        only very sparsely filled.

    Other Parameters:
      name (str):  The name of this stage (handled automatically by fast-flow)
      out_dir (str):  Where to put the summary table (handled automatically by
          fast-flow)

    Raises:
      BadBinnedDataframeConfig: If there is an issue with the binning description.

    """

    def __init__(self, name, out_dir, binning, weights=None, dataset_col=True, pad_missing=False):
        self.name = name
        self.out_dir = out_dir
        ins, outs, binnings = cfg.create_binning_list(self.name, binning)
        self._bin_dims = ins
        self._out_bin_dims = outs
        self._binnings = binnings
        self._dataset_col = dataset_col
        self._weights = cfg.create_weights(self.name, weights)
        self._pad_missing = pad_missing
        self.contents = None

    def collector(self):
        outfilename = "tbl_"
        if self._dataset_col:
            outfilename += "dataset."
        outfilename += ".".join(self._out_bin_dims)
        outfilename += "--" + self.name
        outfilename += ".csv"
        outfilename = os.path.join(self.out_dir, outfilename)
        binnings = None
        if self._pad_missing:
            binnings = dict(zip(self._out_bin_dims, self._binnings))
        return Collector(outfilename, self._dataset_col, binnings=binnings)

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


count_label = "n"
weight_labels = ("sumw", "sumw2")


def _make_column_labels(weights):
    labels = [w + ":" + l for l in weight_labels for w in weights]
    return [count_label] + labels


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
        data[out_dimension] = pd.cut(data[dimension], binning, right=False)
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
        histogram = counts.to_frame(count_label)

    histogram.index.set_names(out_dimensions, inplace=True)
    return histogram
