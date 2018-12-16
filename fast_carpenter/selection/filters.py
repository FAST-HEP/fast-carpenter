import six
import numpy as np
from ..expressions import evaluate
from ..define.reductions import get_awkward_reduction


class Counter():
    def __init__(self, weights):
        self._weights = weights
        self._w_counts = np.zeros(len(weights))
        self._counts = 0

    @staticmethod
    def get_unweighted_increment(data, mask):
        if mask is None:
            return len(data)
        elif mask.dtype.kind == "b":
            return np.count_nonzero(mask)
        else:
            return len(mask)

    @staticmethod
    def get_weighted_increment(weight_names, data, mask):
        weights = data.arrays(weight_names, outputtype=lambda *args: np.array(args))
        if mask is not None:
            weights = weights[:, mask]
        return weights.sum(axis=1)

    def increment(self, data, is_mc, mask=None):
        unweighted_increment = self.get_unweighted_increment(data, mask)
        self._counts += unweighted_increment

        if not self._weights:
            return

        if not is_mc:
            self._w_counts += unweighted_increment
            return

        weighted_increments = self.get_weighted_increment(self._weights, data, mask)
        self._w_counts += weighted_increments

    @property
    def counts(self):
        return (self._counts,) + tuple(self._w_counts)

    def add(self, rhs):
        self._w_counts += rhs._w_counts
        self._counts += rhs._counts


class BaseFilter(object):

    def __init__(self, selection, depth, weights):
        self.selection = selection
        self.depth = depth
        self.passed_incl = Counter(weights)
        self.totals_excl = Counter(weights)
        self.passed_excl = Counter(weights)
        self.weights = weights

    def results(self):
        output = (self.depth, str(self))
        output += self.passed_incl.counts + self.passed_excl.counts + self.totals_excl.counts
        output = [output]
        if isinstance(self.selection, list):
            output += sum([sel.results() for sel in self.selection], [])
        return output

    def results_header(self):
        nweights = len(self.weights) + 1
        row1 = ["depth", "cut"]
        row1 += ["passed_incl"] * nweights
        row1 += ["passed_excl"] * nweights
        row1 += ["totals_excl"] * nweights
        row2 = ["", ""] + (["unweighted"] + self.weights) * 3
        return [row1, row2]

    def cut_order(self):
        output = [str(self)]
        if isinstance(self.selection, list):
            output += sum([sel.cut_order() for sel in self.selection], [])
        return output

    def merge(self, rhs):
        self.totals_excl.add(rhs.totals_excl)
        self.passed_incl.add(rhs.passed_incl)
        if isinstance(self.selection, list):
            for sub_lhs, sub_rhs in zip(self.selection, rhs.selection):
                sub_lhs.merge(sub_rhs)


class ReduceSingleCut(BaseFilter):
    def __init__(self, stage_name, depth, weights, **selection):
        super(ReduceSingleCut, self).__init__(selection, depth, weights)
        self._str = str(selection)
        self.reduction = get_awkward_reduction(stage_name,
                                               selection.pop("reduce"),
                                               fill_missing=False)
        self.formula = selection.pop("formula")

    def __call__(self, data, is_mc, current_mask=None):
        self.totals_excl.increment(data, is_mc, mask=current_mask)
        mask = evaluate(data, self.formula)
        mask = self.reduction(mask)

        self.passed_incl.increment(data, is_mc, mask)

        excl_mask = mask if current_mask is None else mask & current_mask
        self.passed_excl.increment(data, is_mc, excl_mask)
        return mask

    def __str__(self):
        return self._str


class SingleCut(BaseFilter):
    def __call__(self, data, is_mc, current_mask=None):
        self.totals_excl.increment(data, is_mc, mask=current_mask)
        mask = evaluate(data, self.selection)

        self.passed_incl.increment(data, is_mc, mask)

        excl_mask = mask if current_mask is None else mask & current_mask
        self.passed_excl.increment(data, is_mc, excl_mask)
        return mask

    def __str__(self):
        return self.selection


class All(BaseFilter):
    def __call__(self, data, is_mc, current_mask=None):
        self.totals_excl.increment(data, is_mc, mask=current_mask)
        mask = np.ones(len(data), dtype=bool)
        excl_mask = mask if current_mask is None else current_mask
        for sel in self.selection:
            new_mask = sel(data, is_mc, current_mask=excl_mask)
            mask &= new_mask
            excl_mask = mask if current_mask is None else mask & current_mask
        self.passed_excl.increment(data, is_mc, excl_mask)
        self.passed_incl.increment(data, is_mc, mask)
        return mask

    def __str__(self):
        return "All"


class Any(BaseFilter):
    def __call__(self, data, is_mc, current_mask=None):
        self.totals_excl.increment(data, is_mc, mask=current_mask)
        mask = np.zeros(len(data), dtype=bool)
        for sel in self.selection:
            new_mask = sel(data, is_mc, current_mask=mask)
            mask |= new_mask
        self.passed_incl.increment(data, is_mc, mask)
        return mask

    def __str__(self):
        return "Any"


def build_selection(stage_name, config, weights=[], depth=0):
    if isinstance(config, six.string_types):
        return SingleCut(config, depth, weights)
    if not isinstance(config, dict):
        raise RuntimeError(stage_name + ": Selection config not a dict")
    if len(config) == 2:
        return ReduceSingleCut(stage_name, depth, weights, **config)
    elif len(config) != 1:
        raise RuntimeError(stage_name + ":Selection config has too many keys")

    method, selections = tuple(config.items())[0]
    if method not in ("All", "Any"):
        raise RuntimeError(stage_name + ": Unknown selection combination method," + method)

    selections = [build_selection(stage_name, sel, weights, depth + 1) for sel in selections]
    if method == "All":
        return All(selections, depth, weights)
    if method == "Any":
        return Any(selections, depth, weights)
