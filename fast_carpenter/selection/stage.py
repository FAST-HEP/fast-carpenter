"""Stages to remove events from subsequent stages

Provides two stages:

  * :class:`CutFlow` -- Prevent subsequent stages from seeing certain events,
  * :class:`SelectPhaseSpace` -- Create a new variable which can be used as a mask

Both stages are configured very similarly, and both stages produce an output
table describing how many events pass each subsequent cut to make it into the
final mask.
"""
from __future__ import absolute_import
import six
import pandas as pd
import os
from copy import deepcopy
from .filters import build_selection


__all__ = ["CutFlow", "SelectPhaseSpace"]


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
    """Prevents subsequent stages seeing certain events.

    The two most important  parameters to understand are the ``selection`` and
    ``weights`` parameters.

    Parameters:
      selection (str or dict): The criteria for selecting events, formed by a
        nested set of "cuts".  Each cut must either be a valid :ref:`expressions`
        or a single-length dictionary, with one of ``Any`` or ``All`` as the key,
        and a list of cuts as the value.
      weights (str or list[str], dict[str, str]): How to weight events in the
        output summary table.  Must be either a single variable, a list of
        variables, or a dictionary where the values are variables in the data and
        keys are the column names that these weights should be called in the
        output tables.

    Example:
      Mask events using a single cut based on the ``nJet`` variable being
      greater than 2 and weight events in the summary table by the
      ``EventWeight`` variable::

         cut_flow_1:
             selection:
                 nJet > 2
             weights: EventWeight

      Mask events by requiring both the ``nMuon`` variable being greater than 2
      and the first ``Muon_energy`` value in each event being above 20.  Don't weight
      events in the summary table::

         cut_flow_2:
             selection:
                 All:
                   - nMuon > 2
                   - {reduce: 0, formula: Muon_energy > 20}

      Mask events by requiring the ``nMuon`` variable be greater than 2 and
      either the first ``Muon_energy`` value in each event is above 20 or the
      ``total_energy`` is greater than 100.  The summary table will weight
      events by both the EventWeight variable (called weight_nominal in the
      table) and the SystUp variable (called weight_syst_up in the summary)::

         cut_flow_3:
             selection:
                 All:
                   - nMuon > 2
                   - Any:
                     - {reduce: 0, formula: Muon_energy > 20}
                     - total_energy > 100
             weights: {weight_nominal: EventWeight, weight_syst_up: SystUp}


    Other Parameters:
      name (str):  The name of this stage (handled automatically by fast-flow)
      out_dir (str):  Where to put the summary table (handled automatically by
          fast-flow)
      selection_file (str): Deprecated
      keep_unique_id (bool): If ``True``, the summary table will contain a
          column that gives each cut a unique id.  This is used internally to
          maintain the cut order, and often will not be useful in subsequent
          manipulation of the output table, so by default this is removed.
      counter (bool): Currently unused

    Raises:
      BadCutflowConfig: If neither or both of ``selection`` and
          ``selection_file`` are provided, or if a bad selection or
          weight configuration is given.


    See Also:
      :class:`SelectPhaseSpace`: Adds the resulting event-mask as a new
      variable to the data.

      :meth:`selection.filters.build_selection`: Handles the actual creation of
      the event selection, based on the configuration.

      `numexpr <https://numexpr.readthedocs.io/en/latest/>`_: which is used for
      the internal expression handling.

    """
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
    """Create an event-mask and add it to the data-space.

    This is identical to the :class:`CutFlow` class, except that the resulting
    mask is added to the list of variables in the data-space, rather than being
    used directly to remove events.  This allows multiple "regions" to be defined
    using different CutFlows in a single configuration.

    Parameters:
      region_name: The name given to the resulting mask when added to back to
        the data-space.

    See Also:
      :class:`CutFlow`: for a description of the other parameters.
    """
    def __init__(self, name, out_dir, region_name, **kwargs):
        super(SelectPhaseSpace, self).__init__(name, out_dir, **kwargs)
        self.region_name = region_name

    def event(self, chunk):
        is_mc = chunk.config.dataset.eventtype == "mc"
        new_mask = self.selection(chunk.tree, is_mc)
        chunk.tree.new_variable(self.region_name, new_mask)
