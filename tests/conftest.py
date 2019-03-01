import pytest
import uproot
from collections import namedtuple


@pytest.fixture()
def infile():
    filename = "tests/data/CMS_HEP_tutorial_ww.root"
    return uproot.open(filename)["events"]


FakeEventRange = namedtuple("FakeEventRange", "start_entry stop_entry entries_in_block")


@pytest.fixture
def wrapped_tree(infile):
    import fast_carpenter.tree_wrapper as tree_w
    event_range = FakeEventRange(100, 200, 100)
    tree = tree_w.WrappedTree(infile, event_range)
    return tree


class Namespace():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeBEEvent(object):
    def __init__(self, tree, eventtype):
        self.tree = tree
        self.config = Namespace(dataset=Namespace(eventtype=eventtype))
