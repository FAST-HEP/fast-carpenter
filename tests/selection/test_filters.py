import pytest
import numpy as np
import uproot
import fast_carpenter.selection.filters as filters


@pytest.fixture
def filename():
    return "tests/data/CMS_HEP_tutorial_ww.root"


@pytest.fixture
def config_1():
    return "NMuon > 1"


def test_build_selection_1(config_1):
    selection = filters.build_selection("test_build_selection_1", config_1)
    assert isinstance(selection, filters.SingleCut)


def test_selection_1(config_1, filename):
    selection = filters.build_selection("test_selection_1", config_1)
    infile = uproot.open(filename)["events"]
    mask = selection(infile)
    assert np.count_nonzero(mask) == 289

    result = selection.results()
    assert len(result) == 1
    assert result[0][0] == 0
    assert result[0][1] == "NMuon > 1"
    assert result[0][2] == 289


@pytest.fixture
def config_2():
    return {"Any": ["NMuon > 1", "NElectron > 1", "NJet > 1"]}


def test_selection_2_weights(config_2, filename):
    selection = filters.build_selection("test_selection_1",
                                        config_2, weights=["EventWeight"])
    infile = uproot.open(filename)["events"]
    mask = selection(infile)
    assert np.count_nonzero(mask) == 1486

    header = selection.results_header()
    assert len(header) == 2
    assert len(header[0]) == 6

    result = selection.results()

    assert len(result) == 4
    assert result[0][0] == 0
    assert result[0][1] == "Any"
    assert result[0][2] == 1486
    assert result[0][3] == np.sum(infile.array("EventWeight")[mask])


@pytest.fixture
def config_3():
    return {"All": ["NMuon > 1", {"Any": ["NElectron > 1", "NJet > 1"]}]}


def test_selection_3(config_3, filename):
    selection = filters.build_selection("test_selection_3", config_3)
    infile = uproot.open(filename)["events"]
    mask = selection(infile)
    assert np.count_nonzero(mask) == 8

    result = selection.results()
    assert len(result) == 5
    assert result[0][0] == 0
    assert result[0][1] == "All"
    assert result[0][2] == 8
    assert result[1][0] == 1
    assert result[1][1] == "NMuon > 1"
    assert result[1][2] == 289


@pytest.fixture
def config_jagged():
    return "subentry == 1 and Muon_Px > 0.3"


def test_selection_jagged(config_jagged, filename):
    selection = filters.build_selection("test_selection_jagged", config_jagged)
    infile = uproot.open(filename)["events"]
    mask = selection(infile)
    assert np.count_nonzero(mask) == 144
