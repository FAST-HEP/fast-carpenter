"""
"""
import six
import numpy as np
from .expressions import get_branches
from awkward import JaggedArray
from . import reductions
import numba


class BadVariablesConfig(Exception):
    pass


class Collector():
    def collect(self, *args, **kwargs):
        pass


@numba.njit
def parents2startsstops(parents):
    changes = np.zeros(parents.max() + 2, dtype=numba.int32)
    count = 0
    last_parent = 0
    out_change = 1
    for count, parent in enumerate(parents):
        step_parent = parent - last_parent
        while step_parent > 0:
            changes[out_change] = count
            out_change += 1
            step_parent -= 1
        last_parent = parent
    return changes[:-1], changes[1:]


class Define():

    def __init__(self, name, out_dir, **variables):
        self.name = name
        self.out_dir = out_dir
        self._variables = _build_calculations(variables)

    def collector(self):
        return Collector()

    def begin(self, event):
        self.contents = None

    def event(self, chunk):
        allkeys = chunk.tree.allkeys()
        for output, (expression, reduction, fill_missing) in self._variables.items():
            branches = get_branches(expression, allkeys)
            data = chunk.tree.pandas.df(branches)
            result = data.eval(expression)
            if reduction:
                groups = result.groupby(level=0)
                result = groups.aggregate(reduction)
                result.index.set_levels(np.concatenate(groups.indices.values()), 0)
                result = result.reindex(np.arange(len(data))).fillna(fill_missing)
                starts, stops = parents2startsstops()
            else:
                starts, stops = parents2startsstops(result.index.get_level_values(0).values)
            array = JaggedArray(starts, stops, result.values)
            chunk.tree.new_variable(output, array)
        return True


def _build_calculations(variables):
    return {name: _build_one_calc(name, config) for name, config in variables.items()}


def _build_one_calc(name, config):
    if isinstance(config, six.string_types):
        return (config, None, None)
    if not isinstance(config, dict):
        raise RuntimeError("To define a new variable need either a string for just a formula or a dictionary")
    if [key for key in config.keys() if key not in ("reduce", "formula", "fill_missing")]:
        raise RuntimeError("unknown parameter parsed defining variable '{}'".format(name))
    reduction = getattr(reductions, config["reduction"]) if "reduction" in config else None
    return config["formula"], reduction, config.get("fill_missing", float("NaN"))
