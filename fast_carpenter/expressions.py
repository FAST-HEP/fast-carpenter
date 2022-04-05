import numpy as np
import re
import tokenize
import awkward0
import awkward as ak
import logging

from io import StringIO

from typing import List

logger = logging.getLogger(__name__)


__all__ = ["get_branches", "evaluate"]


constants = {"nan": np.nan,
             "inf": np.inf,
             "pi": np.pi,
             "e": np.e,
             }


def get_branches(cut: str, valid: List[str]) -> List[str]:
    """ Get branches relevant to the cut. """
    branches = []
    string = StringIO(cut).readline
    tokens = tokenize.generate_tokens(string)
    current_branch = []
    for toknum, tokval, _, _, _ in tokens:
        if toknum == tokenize.NAME:
            if ".".join(current_branch + [tokval]) in valid:
                current_branch.append(tokval)
                continue
        if tokval == ".":
            continue
        if current_branch:
            branches.append(".".join(current_branch))
            current_branch = []
    return branches


def deconstruct_jaggedness(array, counts):
    if not isinstance(array, (awkward0.array.base.AwkwardArrayWithContent, ak.highlevel.Array)):
        return array, counts

    if isinstance(array, ak.highlevel.Array):
        counts.insert(0, array.layout.compact_offsets64)
        array = ak.flatten(array)
        return deconstruct_jaggedness(array.layout.content, counts)
    else:
        array = array.compact()
        counts.insert(0, array.counts)
    return deconstruct_jaggedness(array.content, counts)


def reconstruct_jaggedness(array, counts):
    # array = ak.unflatten(array, lengths, axis=1)
    for count in counts:
        array = awkward0.JaggedArray.fromcounts(count, array)
    return array


attribute_re = re.compile(r"([a-zA-Z]\w*)\s*(\.\s*(\w+))+")


def preprocess_expression(expression):
    alias_dict = {}
    replace_dict = {}
    for match in attribute_re.finditer(expression):
        original = match.group(0)
        alias = original.replace('.', '__DOT__')
        alias_dict[alias] = original
        replace_dict[original] = alias
    clean_expr = attribute_re.sub(lambda x: replace_dict[x.group(0)], expression)
    return clean_expr, alias_dict


def evaluate(tree, expression):
    cleaned_expression, _ = preprocess_expression(expression)
    return tree.evaluate(cleaned_expression, global_dict=constants)
