"""
"""
import six
from .expressions import get_branches
from awkward import JaggedArray
from . import reductions


class BadVariablesConfig(Exception):
    pass


class Define():

    def __init__(self, name, out_dir, variables):
        self.name = name
        self.out_dir = out_dir
        self._variables = _build_calculations(name, variables)

    def event(self, chunk):
        for output, expression, reduction, reduction_args, fill_missing in self._variables:
            branches = get_branches(expression, chunk.tree.allkeys())
            data = chunk.tree.pandas.df(branches)
            result = data.eval(expression)
            if reduction:
                groups = result.groupby(level=0)
                result = groups.agg(reduction, **reduction_args)
                array = result.values
            else:
                events = result.index.get_level_values(0).values
                events -= events[0]
                array = JaggedArray.fromparents(events, result.values)

            chunk.tree.new_variable(output, array)
        return True


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
    default = [name, config, None, None, None]
    if isinstance(config, six.string_types):
        return tuple(default)
    if not isinstance(config, dict):
        msg = "{}: To define a new variable need either a string for just a formula or a dictionary"
        raise RuntimeError(msg.format(stage_name))
    if [key for key in config.keys() if key not in ("reduce", "formula", "fill_missing")]:
        msg = "{}: Unknown parameter parsed defining variable '{}'"
        raise RuntimeError(msg.format(stage_name, name))
    reduction = getattr(reductions, config["reduce"]) if "reduce" in config else None
    if isinstance(reduction, tuple):
        reduction, reduction_args = reduction
    else:
        reduction_args = {}
    return name, config["formula"], reduction, reduction_args, config.get("fill_missing", None)
