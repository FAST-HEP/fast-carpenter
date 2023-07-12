
from collections import defaultdict
from copy import deepcopy
import hashlib
from typing import Any, Callable
import awkward as ak
import numpy as np
import pandas as pd
from functools import partial
import re
import dask.bag as db


from ..protocols import ArrayMethodsProtocol, DataMapping
from ..utils import register_in_collection, unregister_from_collection

from .ast import SymbolNode, Transformer, ASTWrapper, FunctionNode, expression_to_ast
from .custom import CONSTANTS, SUPPORTED_FUNCTIONS


ATTR_REGEX = re.compile(r"([a-zA-Z]\w*)\s*(\.\s*(\w+))+")


register_function = partial(register_in_collection, SUPPORTED_FUNCTIONS, "Supported Functions")
unregister_function = partial(unregister_from_collection, SUPPORTED_FUNCTIONS, "Supported Functions")


def _replace_attributes(expression: str):
    """Replace attributes in an expression with the correct variable names.

    Args:
        expression (str): The expression to replace attributes in.

    Returns:
        The expression with attributes replaced.
    """
    def _replace(match):
        """Replace a match with the correct variable name."""
        # match.group(1) is the first group, which is the first variable name
        # match.group(3) is the third group, which is the attribute name
        return f"{match.group(1)}__DOT__{match.group(3)}"

    return ATTR_REGEX.sub(_replace, expression)


def _build_global_dict():
    """Build the global dictionary for the expression."""
    global_dict = {}
    global_dict.update(CONSTANTS)
    global_dict.update(SUPPORTED_FUNCTIONS)
    return global_dict


def process_expression(data: DataMapping, expression: str):
    """Process an expression using the data.

    Args:
        data (DataMapping): The data to use in the expression.
        expression (str): The expression to process.

    Returns:
        The result of the expression.
    """
    global_dict = _build_global_dict()
    # register_function("eval", partial(data.evaluate, global_dict=global_dict))
    # from dask.threaded import get
    # get(dsk, '<last task>')  # executes in parallel

    return data.evaluate(expression, global_dict=global_dict)



__all__ = [
    "process_expression",
    "expression_to_ast",
    "SymbolNode",
    "Transformer",
    "FunctionNode",
    "AstWrapper",
]