import six
from typing import List, Tuple

import numpy as np
import pandas as pd
from ..expressions import evaluate
from ..define.reductions import get_awkward_reduction
from ..tree_adapter import ArrayMethods
from ..weights import extract_weights, get_unweighted_increment, get_weighted_increment


def safe_and(left, right):
    if left is None:
        return right
    if right is None:
        return left
    return left & right


def safe_or(left, right):
    if left is None:
        return right
    if right is None:
        return left
    return left | right


class Counter():
    _weight_names: List[str]
    _w_counts: np.ndarray
    _counts: int

    def __init__(self, weight_names: List[str]) -> None:
        self._weight_names = weight_names
        self._w_counts = np.zeros(len(weight_names))
        self._counts = 0

    # TODO: increment should take weights, not data
    def increment(self, data, is_mc, mask=None):
        weights = extract_weights(data, self._weight_names)

        try:
            unweighted_increment = get_unweighted_increment(weights, mask)
        except ValueError:
            unweighted_increment = len(data)
        self._counts += unweighted_increment

        if not self._weight_names or not is_mc:
            self._w_counts += unweighted_increment
            return

        weighted_increments = get_weighted_increment(weights, mask)
        self._w_counts = ArrayMethods.sum([self._w_counts, weighted_increments], axis=0).to_numpy()

    @property
    def counts(self) -> Tuple[int, float]:
        if not self._weight_names:
            return (self._counts,)
        return (self._counts,) + tuple(self._w_counts)

    def add(self, rhs) -> None:
        self._w_counts = (np.sum(self._w_counts + rhs._w_counts).tolist(),)
        self._counts += rhs._counts


class BaseFilter(object):

    def __init__(self, selection, depth, cut_id, weights):
        self._unique_id = ",".join(map(str, cut_id))

        self.selection = selection
        self.depth = depth
        self.passed_excl = Counter(weights)
        self.totals_incl = Counter(weights)
        self.passed_incl = Counter(weights)
        self.weights = weights

    @property
    def index_values(self):
        output = [(self._unique_id, self.depth, str(self))]
        if isinstance(self.selection, list):
            output = sum([sel.index_values for sel in self.selection], output)
        return output

    @property
    def values(self):
        output = [self.passed_excl.counts + self.passed_incl.counts + self.totals_incl.counts]
        if isinstance(self.selection, list):
            output += sum([sel.values for sel in self.selection], [])
        return output

    @property
    def columns(self):
        nweights = len(self.weights) + 1
        row1 = ["passed_only_cut"] * nweights
        row1 += ["passed_incl"] * nweights
        row1 += ["totals_incl"] * nweights
        row2 = (["unweighted"] + self.weights) * 3
        return [row1, row2]

    def to_dataframe(self):
        index_names = ("unique_id", "depth", "cut")
        index = pd.MultiIndex.from_tuples(self.index_values, names=index_names)
        columns = pd.MultiIndex.from_arrays(self.columns)
        return pd.DataFrame(self.values, columns=columns, index=index)

    def merge(self, rhs):
        self.totals_incl.add(rhs.totals_incl)
        self.passed_incl.add(rhs.passed_incl)
        self.passed_excl.add(rhs.passed_excl)
        if isinstance(self.selection, list):
            for sub_lhs, sub_rhs in zip(self.selection, rhs.selection):
                sub_lhs.merge(sub_rhs)
        return self

    def increment_counters(self, data, is_mc, excl, before, after):
        self.passed_excl.increment(data, is_mc, excl)
        self.passed_incl.increment(data, is_mc, after)
        self.totals_incl.increment(data, is_mc, before)

    def __repr__(self):
        rep = ": {!r}"
        if isinstance(self.selection, list):
            rep = ": [{!r}]"
        rep = self.__class__.__name__ + rep.format(self.selection)
        return rep


class ReduceSingleCut(BaseFilter):
    def __init__(self, stage_name, depth, cut_id, weights, selection):
        super(ReduceSingleCut, self).__init__(selection, depth, cut_id, weights)
        self._str = str(selection)
        self.reduction = get_awkward_reduction(
            stage_name,
            selection.get("reduce"),
            fill_missing=False,
        )
        self.formula = selection.get("formula")

    def __call__(self, data, is_mc, **kwargs):
        mask = evaluate(data, self.formula)
        mask = self.reduction(mask)
        return mask

    def __str__(self):
        return self._str


class SingleCut(BaseFilter):
    def __call__(self, data, is_mc, **kwargs):
        mask = evaluate(data, self.selection)
        return mask

    def __str__(self):
        return self.selection


class All(BaseFilter):
    def __call__(self, data, is_mc,
                 current_mask=None, combine_op=safe_and):
        mask = np.ones(len(data), dtype=bool)
        for sel in self.selection:
            excl_mask = sel(data, is_mc,
                            current_mask=combine_op(current_mask, mask),
                            combine_op=safe_and)
            new_mask = mask & excl_mask
            sel.increment_counters(data, is_mc, excl=excl_mask,
                                   after=new_mask, before=mask)
            mask = new_mask
        return mask

    def __str__(self):
        return "All"


class Any(BaseFilter):
    def __call__(self, data, is_mc,
                 current_mask=None, combine_op=safe_or):
        mask = np.zeros(len(data), dtype=bool)
        for sel in self.selection:
            excl_mask = sel(data, is_mc,
                            current_mask=current_mask,
                            combine_op=combine_op)
            new_mask = mask | excl_mask
            sel.increment_counters(data, is_mc, excl=excl_mask,
                                   after=combine_op(new_mask, current_mask),
                                   before=current_mask)
            mask = new_mask
        return mask

    def __str__(self):
        return "Any"


class OuterCounterIncrementer(BaseFilter):
    def __init__(self, *args, **kwargs):
        super(OuterCounterIncrementer, self).__init__(*args, **kwargs)
        self._wrapped_selection = BaseFilter.__getattribute__(self, "selection")

    def __call__(self, data, is_mc):
        mask = self._wrapped_selection(data, is_mc)
        self._wrapped_selection.increment_counters(data, is_mc, excl=mask, after=mask, before=None)
        return mask

    def __getattribute__(self, name):
        if name in ["__call__", "_wrapped_selection"]:
            return BaseFilter.__getattribute__(self, name)
        return BaseFilter.__getattribute__(self, "selection").__getattribute__(name)


def build_selection(stage_name, config, weights=[]):
    """Creates event selectors based on the configuration.

    Parameters:
        stage_name: Used to help in error messages.
        config: The event selection configuration.
        weights: How to weight events, used to produce the resulting cut
            efficiency table.

    Raises:
        RuntimeError: if any of the configurations are invalid.
    """
    selection = handle_config(stage_name, config, weights)
    return OuterCounterIncrementer(selection, depth=-1, cut_id=[-1], weights=weights)


def handle_config(stage_name, config, weights, depth=0, cut_id=[0]):
    if isinstance(config, six.string_types):
        return SingleCut(config, depth, cut_id, weights)
    if not isinstance(config, dict):
        raise RuntimeError(stage_name + ": Selection config not a dict")
    if len(config) == 2:
        return ReduceSingleCut(stage_name, depth, cut_id, weights, config)
    elif len(config) != 1:
        raise RuntimeError(stage_name + ":Selection config has too many keys")

    method, in_selections = tuple(config.items())[0]
    if method not in ("All", "Any"):
        raise RuntimeError(stage_name + ": Unknown selection combination method," + method)

    selections = []
    for i, sel in enumerate(in_selections):
        cut = handle_config(stage_name, sel, weights, depth + 1, cut_id=cut_id + [i])
        selections.append(cut)
    if method == "All":
        return All(selections, depth, cut_id, weights)
    if method == "Any":
        return Any(selections, depth, cut_id, weights)
