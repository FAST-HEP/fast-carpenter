import pytest
import uproot
import fast_carpenter.selection.stage as stage
import fast_carpenter.selection.filters as filters
from fast_carpenter.masked_tree import MaskedUprootTree


@pytest.fixture
def infile():
    filename = "tests/data/CMS_HEP_tutorial_ww.root"
    return uproot.open(filename)["events"]


def test__create_weights_none():
    weights = stage._create_weights("test__create_weights_none", None)
    assert isinstance(weights, dict)
    assert len(weights) == 0


def test__create_weights_single_string():
    weights = "dummy_weight"
    weights = stage._create_weights("test__create_weights_none", weights)
    assert isinstance(weights, dict)
    assert len(weights) == 1
    assert list(weights.keys()) == ["dummy_weight"]
    assert all([k == v for k, v in weights.items()])


def test__create_weights_list():
    weights = ["hello", "world"]
    weights = stage._create_weights("test__create_weights_none", weights)
    assert isinstance(weights, dict)
    assert len(weights) == 2
    assert list(sorted(weights.keys())) == sorted(["hello", "world"])
    assert all([k == v for k, v in weights.items()])


def test__create_weights_dict():
    weights = {"HEY": "hello", "EARTH": "world"}
    weights = stage._create_weights("test__create_weights_none", weights)
    assert isinstance(weights, dict)
    assert len(weights) == 2
    assert list(sorted(weights.keys())) == sorted(["HEY", "EARTH"])
    assert list(sorted(weights.values())) == sorted(["hello", "world"])


@pytest.fixture
def cutflow_1():
    return stage.CutFlow("cutflow_1", "somewhere", selection="NMuon > 1", weights="NElectron")


def test_cutflow_1(cutflow_1):
    assert cutflow_1._weights == {"NElectron": "NElectron"}
    assert isinstance(cutflow_1.selection, filters.SingleCut)


class FakeBEEvent(object):
    def __init__(self, tree):
        self.tree = MaskedUprootTree(tree)


class Namespace():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_cutflow_1_executes_mc(cutflow_1, infile):
    chunk = FakeBEEvent(infile)
    chunk.config = Namespace(dataset=Namespace(eventtype="mc"))
    cutflow_1.event(chunk)

    assert len(chunk.tree) == 289

    collector = cutflow_1.collector()
    assert collector.filename == "somewhere/cuts_cutflow_1-NElectron.csv"


def test_cutflow_1_executes_data(cutflow_1, infile):
    chunk = FakeBEEvent(infile)
    chunk.config = Namespace(dataset=Namespace(eventtype="data"))
    cutflow_1.event(chunk)

    assert len(chunk.tree) == 289

    collector = cutflow_1.collector()
    assert collector.filename == "somewhere/cuts_cutflow_1-NElectron.csv"


# def test__load_selection_file(stage_name, selection_file):
