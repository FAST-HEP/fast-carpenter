import pytest
import uproot
import fast_chainsaw.selection.stage as stage
import fast_chainsaw.selection.filters as filters


@pytest.fixture
def infile():
    filename = "test/data/CMS_HEP_tutorial_ww.root"
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
    assert weights.keys() == ["dummy_weight"]
    assert weights.keys() == weights.values()


def test__create_weights_list():
    weights = ["hello", "world"]
    weights = stage._create_weights("test__create_weights_none", weights)
    assert isinstance(weights, dict)
    assert len(weights) == 2
    assert sorted(weights.keys()) == sorted(["hello", "world"])
    assert weights.keys() == weights.values()


def test__create_weights_dict():
    weights = {"HEY": "hello", "EARTH": "world"}
    weights = stage._create_weights("test__create_weights_none", weights)
    assert isinstance(weights, dict)
    assert len(weights) == 2
    assert sorted(weights.keys()) == sorted(["HEY", "EARTH"])
    assert sorted(weights.values()) == sorted(["hello", "world"])


@pytest.fixture
def cutflow_1():
    return stage.CutFlow("cutflow_1", "somewhere", selection="NMuon > 1", weights="NElectron")


def test_cutflow_1(cutflow_1):
    assert cutflow_1._weights == {"NElectron": "NElectron"}
    assert isinstance(cutflow_1.selection, filters.SingleCut)


class FakeBEEvent(object):
    def __init__(self, tree):
        self.tree = tree


def test_cutflow_1_executes(cutflow_1, infile):
    chunk = FakeBEEvent(infile)
    cutflow_1.event(chunk)

    assert len(chunk.event_mask) == 289

    collector = cutflow_1.collector()
    assert collector.filename == "somewhere/cuts_cutflow_1-NElectron.csv"

# def test__load_selection_file(stage_name, selection_file):
