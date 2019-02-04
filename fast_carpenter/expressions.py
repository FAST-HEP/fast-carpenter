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
    def __init__(self, tree):
        self.tree = tree
        self.starts = None
        self.stops = None

    def __getitem__(self, item):
        array = self.tree.array(item)
        starts = getattr(array, "starts", None)
        if starts is not None:
            self.set_starts_stop(starts, array.stops)
            return array.content
        return array

    def __contains__(self, item):
        return item in self.tree

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


def evaluate(tree, expression):
    adaptor = TreeToDictAdaptor(tree)
    result = numexpr.evaluate(expression, local_dict=adaptor)
    if adaptor.starts is not None:
        result = awkward.JaggedArray(adaptor.starts, adaptor.stops, result)
    return result
