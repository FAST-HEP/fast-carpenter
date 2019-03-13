import aghast
import os
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


class BinnedDataframeAghast:

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


def _bin_values(data, dimensions, binnings, weights, out_dimensions=None, out_weights=None):
    if not out_dimensions:
        out_dimensions = dimensions
    if not out_weights:
        out_weights = weights
