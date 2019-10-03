import numpy as np
import re
import numexpr
import tokenize
import awkward
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


__all__ = ["get_branches", "evaluate"]


constants = {"nan": np.nan,
             "inf": np.inf,
             "pi": np.pi,
             "e": np.e,
             }


def get_branches(cut, valid):
    valid = [v.decode("utf-8") for v in valid]

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
    if not isinstance(array, awkward.array.base.AwkwardArrayWithContent):
        return array, counts

    array.compact()
    counts.insert(0, array.counts)
    return deconstruct_jaggedness(array.content, counts)


def reconstruct_jaggedness(array, counts):
    for count in counts:
        array = awkward.JaggedArray.fromcounts(count, array)
    return array


class TreeToDictAdaptor():
    """
    Make an uproot tree look like a dict for numexpr
    """
    def __init__(self, tree, alias_dict):
        self.tree = tree
        self.counts = None
        self.aliases = alias_dict

    def __getitem__(self, item):
        if item in constants:
            return constants[item]
        full_item = self.aliases.get(item, item)
        array = self.tree.array(full_item)
        array = self.strip_jaggedness(array)
        return array

    def __contains__(self, item):
        return item in self.tree or item in self.aliases

    def __iter__(self):
        for i in self.tree:
            yield i

    def strip_jaggedness(self, array):
        array, new_counts = deconstruct_jaggedness(array, counts=[])
        if self.counts is not None:
            if not np.array_equal(self.counts, new_counts):
                raise RuntimeError("Operation using arrays with different jaggedness")
        else:
            self.counts = new_counts
        return array

    def apply_jaggedness(self, array):
        if self.counts is None:
            return array
        result = reconstruct_jaggedness(array, self.counts)
        return result


attribute_re = re.compile(r"([a-zA-Z]\w*)\s*\.\s*(\w+)")


def preprocess_expression(expression):
    alias_dict = {}
    replace_dict = {}
    for match in attribute_re.finditer(expression):
        original = match.group(0)
        alias = match.expand(r"\1__DOT__\2")
        alias_dict[alias] = original
        replace_dict[original] = alias
    clean_expr = attribute_re.sub(lambda x: replace_dict[x.group(0)], expression)
    return clean_expr, alias_dict


def evaluate(tree, expression):
    cleaned_expression, alias_dict = preprocess_expression(expression)
    adaptor = TreeToDictAdaptor(tree, alias_dict)
    result = numexpr.evaluate(cleaned_expression, local_dict=adaptor)
    result = adaptor.apply_jaggedness(result)
    return result
