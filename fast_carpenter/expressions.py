import re
import numexpr
import tokenize
import awkward
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


__all__ = ["get_branches", "evaluate"]


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


class TreeToDictAdaptor():
    """
    Make an uproot tree look like a dict for numexpr
    """
    def __init__(self, tree, alias_dict):
        self.tree = tree
        self.starts = None
        self.stops = None
        self.aliases = alias_dict

    def __getitem__(self, item):
        full_item = self.aliases.get(item, item)
        array = self.tree.array(full_item)
        starts = getattr(array, "starts", None)
        if starts is not None:
            self.set_starts_stop(starts, array.stops)
            return array.content
        return array

    def __contains__(self, item):
        return item in self.tree or item in self.aliases

    def __iter__(self):
        for i in self.tree:
            yield i

    def set_starts_stop(self, starts, stops):
        if self.starts is not None:
            if any(self.starts != starts) or any(self.stops != stops):
                raise RuntimeError("Mismatched starts and stops")
        else:
            self.starts = starts
            self.stops = stops


attribute_re = re.compile(r"(\w+)\s*\.\s*(\w+)")


def preprocess_expression(expression):
    alias_dict = {}
    replace_dict = {}
    for match in attribute_re.finditer(expression):
        original = match.group(0)
        alias = match.expand(r"\1__\2")
        alias_dict[alias] = original
        replace_dict[original] = alias
    clean_expr = attribute_re.sub(lambda x: replace_dict[x[0]], expression)
    return clean_expr, alias_dict


def evaluate(tree, expression):
    cleaned_expression, alias_dict = preprocess_expression(expression)
    adaptor = TreeToDictAdaptor(tree, alias_dict)
    result = numexpr.evaluate(expression, local_dict=adaptor)
    if adaptor.starts is not None:
        result = awkward.JaggedArray(adaptor.starts, adaptor.stops, result)
    return result
