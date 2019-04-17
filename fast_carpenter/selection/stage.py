"""
"""
from __future__ import absolute_import
import six
import pandas as pd
import os
from copy import deepcopy
from .filters import build_selection


__all__ = ["CutFlow"]


class BadCutflowConfig(Exception):
    pass


class BadSelectionFile(Exception):
    pass


class Collector():
    def __init__(self, filename, keep_unique_id):
        self.filename = filename
        self.keep_unique_id = keep_unique_id

    def collect(self, dataset_readers_list):
        if len(dataset_readers_list) == 0:
            return None

        output = self._prepare_output(dataset_readers_list)
        output.to_csv(self.filename, float_format="%.17g")

    def _prepare_output(self, dataset_readers_list):
        dataset_readers_list = [(d, [r.selection for r in readers])
                                for d, readers in dataset_readers_list
                                if readers]
        if len(dataset_readers_list) == 0:
            return None

        return _merge_data(dataset_readers_list, self.keep_unique_id)


def _merge_data(dataset_readers_list, keep_unique_id=False):
    all_dfs = []
    keys = []
    for dataset, counters in dataset_readers_list:
        output = deepcopy(counters[0])
        for counter in counters[1:]:
            output.merge(counter)
        keys.append(dataset)
        all_dfs.append(output.to_dataframe())

    final_df = pd.concat(all_dfs, keys=keys, names=['dataset'], sort=False)
    if not keep_unique_id:
        final_df.index = final_df.index.droplevel(level="unique_id")

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


class CutFlow(object):
    def __init__(self, name, out_dir, selection_file=None, keep_unique_id=False,
                 selection=None, counter=True, weights=None):
        self.name = name
        self.out_dir = out_dir
        self.keep_unique_id = keep_unique_id
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
        return Collector(outfilename, self.keep_unique_id)

    def event(self, chunk):
        is_mc = chunk.config.dataset.eventtype == "mc"
        new_mask = self.selection(chunk.tree, is_mc)
        chunk.tree.apply_mask(new_mask)

    def merge(self, rhs):
        self.selection.merge(rhs.selection)


class SelectPhaseSpace(CutFlow):
    def __init__(self, name, out_dir, region_name, **kwargs):
        super(SelectPhaseSpace, self).__init__(name, out_dir, **kwargs)
        self.region_name = region_name

    def event(self, chunk):
        is_mc = chunk.config.dataset.eventtype == "mc"
        new_mask = self.selection(chunk.tree, is_mc)
        chunk.tree.new_variable(self.region_name, new_mask)
