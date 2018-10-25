import six
import numpy as np
import pandas as pd
from ..expressions import get_branches


class Counter():
    def __init__(self, weights):
        self._weights = weights
        self._w_counts = np.zeros(len(weights))
        self._counts = 0

    def increment(self, data, mask=None):
        if mask is None:
            self._counts += len(data)
        elif mask.dtype.kind == "b":
            self._counts += np.count_nonzero(mask)
        else:
            self._counts += len(mask)

        if not self._weights:
            return
        if not isinstance(data, pd.DataFrame):
            data = data.pandas.df(self._weights)
        if mask is not None:
            data = data.iloc[mask]
        summed = data[self._weights].sum(axis=0)
        self._w_counts += summed

    @property
    def counts(self):
        return (self._counts,) + tuple(self._w_counts)

    def add(self, rhs):
        self._w_counts += rhs._w_counts
        self._counts += rhs._counts


class BaseFilter():

    def __init__(self, selection, depth, weights):
        self.selection = selection
        self.depth = depth
        self.totals = Counter(weights)
        self.passed = Counter(weights)
        self.weights = weights

    def results(self):
        output = [(self.depth, str(self)) + self.passed.counts + self.totals.counts]
        if isinstance(self.selection, list):
            output += sum([sel.results() for sel in self.selection], [])
        return output

    def results_header(self):
        nweights = len(self.weights) + 1
        header = [["depth", "cut"] + ["passed"] * nweights + ["totals"] * nweights]
        header += [["", "", "unweighted"] + self.weights + ["unweighted"] + self.weights]
        return header

    def merge(self, rhs):
        self.totals.add(rhs.totals)
        self.passed.add(rhs.passed)
        if isinstance(self.selection, list):
            for sub_lhs, sub_rhs in zip(self.selection, rhs.selection):
                sub_lhs.merge(sub_rhs)


class SingleCut(BaseFilter):
    def __call__(self, data):
        branches = get_branches(self.selection, data.allkeys())
        branches += self.weights
        df = data.pandas.df(branches, flatten=True)

        self.totals.increment(df)
        mask = df.eval(self.selection).values
        self.passed.increment(df, mask)
        return mask

    def __str__(self):
        return self.selection


class All(BaseFilter):
    def __call__(self, data):
        self.totals.increment(data)
        mask = np.ones(len(data), dtype=bool)
        for sel in self.selection:
            new_mask = sel(data)
            mask &= new_mask
        self.passed.increment(data, mask)
        return mask

    def __str__(self):
        return "All"


class Any(BaseFilter):
    def __call__(self, data):
        self.totals.increment(data)
        mask = np.zeros(len(data), dtype=bool)
        for sel in self.selection:
            new_mask = sel(data)
            mask |= new_mask
        self.passed.increment(data, mask)
        return mask

    def __str__(self):
        return "Any"


def build_selection(stage_name, config, weights=[], depth=0):
    if isinstance(config, six.string_types):
        return SingleCut(config, depth, weights)
    if not isinstance(config, dict):
        raise RuntimeError(stage_name + ": Selection config not a dict")
    if len(config) != 1:
        raise RuntimeError(stage_name + ":Selection config has too many keys")

    method, selections = tuple(config.items())[0]
    if method not in ("All", "Any"):
        raise RuntimeError(stage_name + ": Unknown selection combination method," + method)

    selections = [build_selection(stage_name, sel, weights, depth + 1) for sel in selections]
    if method == "All":
        return All(selections, depth, weights)
    if method == "Any":
        return Any(selections, depth, weights)
