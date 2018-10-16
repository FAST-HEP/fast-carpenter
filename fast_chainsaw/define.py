"""
"""
import six
import pandas as pd
import numpy as np
import os
from .expressions import get_branches
from awkward import JaggedArray


class BadVariablesConfig(Exception):
    pass


class Collector():
    def collect(self, *args, **kwargs):
        pass


class NumExpr():

    def __init__(self, name, out_dir, **variables):
        self.name = name
        self.out_dir = out_dir
        self._variables = variables

    def collector(self):
        return Collector()

    def begin(self, event):
        self.contents = None

    def event(self, chunk):
        allkeys = chunk.tree.allkeys()
        for output, expression in self._variables.items():
            branches = get_branches(expression, allkeys)
            data = chunk.tree.pandas.df(branches)
            result = data.eval(expression)
            array = JaggedArray.fromparents(result.index.get_level_values(0), result.values)
            chunk.tree.new_variable(output, array)
        return True


def from_subentries(subentries, content):
    offsets = np.nonzero((subentries[1:] - subentries[:-1]) < 0)[0]
    return JaggedArray.fromoffsets(offsets, content)
