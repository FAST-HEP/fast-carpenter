"""
"""
import six
import numpy as np
from awkward import JaggedArray
from ..expressions import get_branches, evaluate
from .reductions import get_pandas_reduction, get_awkward_reduction


class BadVariablesConfig(Exception):
    pass


class Define():

    def __init__(self, name, out_dir, variables):
        self.name = name
        self.out_dir = out_dir
        self._variables = _build_calculations(name, variables, approach="awkward")

    def event(self, chunk):
        for output, expression, reduction, fill_missing in self._variables:
            result = evaluate(chunk.tree, expression)
            if reduction:
                result = reduction(result)

            chunk.tree.new_variable(output, result)
        return True


class DefinePandas():

    def __init__(self, name, out_dir, variables):
        self.name = name
        self.out_dir = out_dir
        self._variables = _build_calculations(name, variables, approach="pandas")

    def event(self, chunk):
        for output, expression, reduction, fill_missing in self._variables:
            branches = get_branches(expression, chunk.tree.allkeys())
            data = chunk.tree.pandas.df(branches)
            result = data.eval(expression)
            if reduction:
                groups = result.groupby(level=0)
                result = reduction(groups)
                array = result.values
            else:
                events = result.index.get_level_values(0).values
                events -= events[0]
                array = JaggedArray.fromparents(events, result.values)

            chunk.tree.new_variable(output, array)
        return True


def _build_calculations(stage_name, variables, approach):
    calculations = []
    for var in variables:
        if not isinstance(var, dict):
            msg = "{}: To define a variable, give me a dictionary for each variable."
            raise RuntimeError(msg.format(stage_name))
        if len(var) != 1:
            msg = "{}: Dictionary to define a variable should have only 1 key-value pair"
            raise RuntimeError(msg.format(stage_name))
        name, config = list(var.items())[0]
        calculations.append(_build_one_calc(stage_name, name, config, approach))
    return calculations


def _build_one_calc(stage_name, name, config, approach):
    reduction = None
    fill_missing = np.nan
    if isinstance(config, six.string_types):
        return name, config, reduction, fill_missing
    if not isinstance(config, dict):
        msg = "{}: To define a new variable need either a string for just a formula or a dictionary"
        raise RuntimeError(msg.format(stage_name))
    if [key for key in config.keys() if key not in ("reduce", "formula", "fill_missing")]:
        msg = "{}: Unknown parameter parsed defining variable '{}'"
        raise RuntimeError(msg.format(stage_name, name))
    if "reduce" in config:
        if approach == "pandas":
            reduction = get_pandas_reduction(stage_name, config["reduce"])
        else:
            reduction = get_awkward_reduction(stage_name, config["reduce"])
    fill_missing = config.get("fill_missing", fill_missing)
    return name, config["formula"], reduction, fill_missing
