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
    """Creates new variables using a string-based expression.

    There are two types of expressions:

      * Simple formulae, and
      * Reducing formulae.

    The essential difference, unfortunately, is an internal one: simple
    expressions are nearly directly handled by `numexpr
    <https://numexpr.readthedocs.io/en/latest/>`_, whereas reducing expressions
    add a layer on top.

    From a users perspective, however, simple expressions are those that
    preserve the dimensionality of the input.  If one of the input variables
    represents a list of values for each event  (whose length might vary), then
    the output will contain an equal-length list of values for each event.

    If, however, a reducing expression is used, then there will be one less
    dimension on the resulting variable.  In this case, if an input variable
    has a list of values for each event, the result of the expression will only
    contain a single value per event.

    Parameters:
      variables (list[dictionary]):  A list of single-length dictionaries whose
          key is the name of the resulting variable, and whose value is the
          expression to create it.


    Other Parameters:
      name (str):  The name of this stage (handled automatically by fast-flow)
      out_dir (str):  Where to put the summary table (handled automatically by
          fast-flow)

    Example:
      ::

        variables:
          - Muon_pt: "sqrt(Muon_px**2 + Muon_py**2)"
          - Muon_is_good: (Muon_iso > 0.3) & (Muon_pt > 10)
          - NGoodMuons: {reduce: count_nonzero, formula: Muon_is_good}
          - First_Muon_pt: {reduce: 0, formula: Muon_pt}

    See Also:
      * :mod:`fast_carpenter.define.reductions`-- for how reductions are handled and exactly what is valid.
      * `numexpr <https://numexpr.readthedocs.io/en/latest/>`_: which is used for
        the internal expression handling.

    """

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
