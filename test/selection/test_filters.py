import pytest
import numpy as np
import uproot
import fast_chainsaw.selection.filters as filters

@pytest.fixture
def filename():
    return "test/data/CMS_HEP_tutorial_ww.root"


@pytest.fixture
def config_1():
    return "NMuon > 1"


@pytest.fixture
def config_2():
    return {"Any": ["NMuon > 1", "NElectron > 1", "NJet > 1"]}


def test_build_selection_1(config_1):
    selection = filters.build_selection("test_build_selection_1", config_1)
    assert isinstance(selection, filters.SingleCut)


def test_selection_1(config_1, filename):
    selection = filters.build_selection("test_selection_1", config_1)
    infile = uproot.open(filename)["events"]
    mask = selection(infile)
    assert np.count_nonzero(mask) == 289


def test_selection_2(config_2, filename):
    selection = filters.build_selection("test_selection_2", config_2)
    infile = uproot.open(filename)["events"]
    mask = selection(infile)
    assert np.count_nonzero(mask) == 1486
