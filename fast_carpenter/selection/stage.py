"""
"""
from __future__ import absolute_import
import six
import pandas as pd
import os
from .filters import build_selection


__all__ = ["CutFlow"]


class BadCutflowConfig(Exception):
    pass


class BadSelectionFile(Exception):
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

        output = self._merge_data(dataset_readers_list)
        output.to_csv(self.filename)

    def _merge_data(self, dataset_readers_list):
        final_df = None
        for dataset, readers in dataset_readers_list:
            for reader in readers:
                header = reader.selection.results_header()
                results = reader.selection.results()
                df = pd.DataFrame(results, columns=pd.MultiIndex.from_arrays(header))
                df.set_index(["depth", "cut"], inplace=True, drop=True)
                df = pd.concat([df], keys=[dataset], names=['dataset'])
                if final_df is None:
                    final_df = df
                    continue
                final_df = final_df.add(df, fill_value=0)

        return final_df


def _load_selection_file(stage_name, selection_file):
    import yaml
    with open(selection_file, "r") as infile:
        cfg = yaml.load(infile)
    if len(cfg) > 1:
        msg = "{}: Selection file has more than one selection"
        raise BadSelectionFile(msg.format(stage_name, selection_file, cfg.keys()))
    return cfg


def _create_weights(stage_name, weights):
    if weights is None:
        return {}
    if isinstance(weights, six.string_types):
        return {weights: weights}

    if isinstance(weights, (tuple, list)):
        weights = {w: w for w in weights}
    if isinstance(weights, dict):
        non_strings = tuple(filter(lambda x: not isinstance(x, six.string_types), weights.values()))
        if non_strings:
            msg = "{}: weight not all string, '{}'"
            raise BadCutflowConfig(msg.format(stage_name, non_strings))
        return weights

    raise BadCutflowConfig("{}: Cannot process weight specification".format(stage_name))


class CutFlow():
    def __init__(self, name, out_dir, selection_file=None,
                 selection=None, counter=True, weights=None):
        self.name = name
        self.out_dir = out_dir
        if not selection and not selection_file:
            raise BadCutflowConfig("{}: Neither selection nor selection_file specified".format(self.name))
        if selection and selection_file:
            raise BadCutflowConfig("{}: Both selection and selection_file given. Choose one!".format(self.name))

        if selection_file:
            selection = _load_selection_file(self.name, selection_file)

        self._counter = counter
        if not self._counter:
            msg = self.name + ": "
            msg += "Optimisations for when no cut-flow counter is required aren't implemented"
            raise NotImplementedError(msg)
        self._weights = None
        if self._counter:
            self._weights = _create_weights(self.name, weights)

        self.selection = build_selection(self.name, selection, weights=list(self._weights.values()))

    def collector(self):
        outfilename = "cuts_"
        outfilename += self.name + "-"
        outfilename += ".".join(self._weights.keys())
        outfilename += ".csv"
        outfilename = os.path.join(self.out_dir, outfilename)
        return Collector(outfilename)

    def event(self, chunk):
        is_mc = chunk.config.dataset.eventtype == "mc"
        new_mask = self.selection(chunk.tree, is_mc)
        chunk.tree.apply_mask(new_mask)

    def merge(self, rhs):
        self.selection.merge(rhs.selection)
