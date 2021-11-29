import pytest
import uproot3
import uproot as uproot4
from collections import namedtuple


@pytest.fixture
def infile():
    filename = "tests/data/CMS_HEP_tutorial_ww.root"
    return uproot3.open(filename)["events"]


@pytest.fixture
def test_input_file():
    return "tests/data/CMS_HEP_tutorial_ww.root"


@pytest.fixture
def uproot3_tree(test_input_file):
    return uproot3.open(test_input_file)["events"]


@pytest.fixture
def uproot4_tree(test_input_file):
    return uproot4.open(test_input_file)["events"]


FakeEventRange = namedtuple("FakeEventRange", "start_entry stop_entry entries_in_block")


@pytest.fixture
def event_range():
    return FakeEventRange(100, 200, 100)


@pytest.fixture
def full_event_range():
    return FakeEventRange(0, 4580, 0)


@pytest.fixture
def wrapped_tree(infile, event_range):
    import fast_carpenter.tree_wrapper as tree_w
    tree = tree_w.WrappedTree(infile, event_range)
    return tree


@pytest.fixture
def full_wrapped_tree(infile, full_event_range):
    import fast_carpenter.tree_wrapper as tree_w
    tree = tree_w.WrappedTree(infile, full_event_range)
    return tree


class Namespace():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeBEEvent(object):
    def __init__(self, tree, eventtype):
        self.tree = tree
        self.config = Namespace(dataset=Namespace(eventtype=eventtype))
