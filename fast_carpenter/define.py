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

    def __init__(self, name, out_dir, variables):
        self.name = name
        self.out_dir = out_dir
        self._variables = _build_calculations(name, variables)

    def collector(self):
        return Collector()

    def begin(self, event):
        self.contents = None

    def event(self, chunk):
        for output, expression, reduction, fill_missing in self._variables:
            branches = get_branches(expression, chunk.tree.allkeys())
            data = chunk.tree.pandas.df(branches)
            result = data.eval(expression)
            if reduction:
                groups = result.groupby(level=0)
                result = groups.aggregate(reduction)
                result = _pad_empty_events(result, groups, fill_missing)
                starts, stops = parents2startsstops(result.index.values)
            else:
                starts, stops = parents2startsstops(result.index.get_level_values(0).values)
            array = JaggedArray(starts, stops, result.values)
            chunk.tree.new_variable(output, array)
        return True


def _pad_empty_events(result, groups, fill_missing):
    values = np.concatenate(groups.indices.values())
    # Pandas GroupBy objects absorb empty events into the proceeding one
    empty = [v[1:] for v in groups.indices.values() if len(v) != 1]
    if empty:
        empty = np.concatenate(empty)
        contained = np.setdiff1d(values, empty)
        result.index = contained
        result = result.reindex(values)
    if fill_missing is not None:
        result = result.fillna(fill_missing)
    return result


def _build_calculations(stage_name, variables):
    calculations = []
    for var in variables:
        if not isinstance(var, dict):
            msg = "{}: To define a variable, give me a dictionary for each variable."
            raise RuntimeError(msg.format(stage_name))
        if len(var) != 1:
            msg = "{}: Dictionary to define a variable should have only 1 key-value pair"
            raise RuntimeError(msg.format(stage_name))
        name, config = list(var.items())[0]
        calculations.append(_build_one_calc(stage_name, name, config))
    return calculations


def _build_one_calc(stage_name, name, config):
    if isinstance(config, six.string_types):
        return (name, config, None, None)
    if not isinstance(config, dict):
        msg = "{}: To define a new variable need either a string for just a formula or a dictionary"
        raise RuntimeError(msg.format(stage_name))
    if [key for key in config.keys() if key not in ("reduce", "formula", "fill_missing")]:
        msg = "{}: Unknown parameter parsed defining variable '{}'"
        raise RuntimeError(msg.format(stage_name, name))
    reduction = getattr(reductions, config["reduce"]) if "reduce" in config else None
    return name, config["formula"], reduction, config.get("fill_missing", None)
