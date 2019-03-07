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
    assert isinstance(selection, filters.OuterCounterIncrementer)
    assert isinstance(selection.selection, filters.SingleCut)


def test_selection_1(config_1, filename):
    selection = filters.build_selection("test_selection_1", config_1)
    infile = uproot.open(filename)["events"]
    mask = selection(infile, is_mc=False)
    assert np.count_nonzero(mask) == 289

    result = selection.results()
    header = selection.results_header()
    assert len(result) == 1
    assert result[0][header[0].index("depth")] == 0
    assert result[0][header[0].index("cut")] == "NMuon > 1"
    assert result[0][header[0].index("passed_incl")] == 289
    assert result[0][header[0].index("passed_only_cut")] == 289
    assert result[0][header[0].index("totals_incl")] == 4580


@pytest.fixture
def config_2():
    return {"Any": ["NMuon > 1", "NElectron > 1", "NJet > 1"]}


def test_selection_2_weights(config_2, filename):
    selection = filters.build_selection("test_selection_1",
                                        config_2, weights=["EventWeight"])
    infile = uproot.open(filename)["events"]
    mask = selection(infile, is_mc=True)
    assert np.count_nonzero(mask) == 1486

    header = selection.results_header()
    assert len(header) == 2
    assert len(header[0]) == 9

    result = selection.results()

    assert len(result) == 4
    assert result[0][header[0].index("depth")] == 0
    assert result[0][header[0].index("cut")] == "Any"
    assert result[0][header[0].index("passed_incl")] == 1486
    assert result[0][header[0].index("passed_incl") + 1] == np.sum(infile.array("EventWeight")[mask])


def test_selection_2_weights_data(config_2, filename):
    selection = filters.build_selection("test_selection_1",
                                        config_2, weights=["EventWeight"])
    infile = uproot.open(filename)["events"]
    mask = selection(infile, is_mc=False)
    assert np.count_nonzero(mask) == 1486

    header = selection.results_header()
    assert len(header) == 2
    assert len(header[0]) == 9

    result = selection.results()

    assert len(result) == 4
    assert result[0][header[0].index("depth")] == 0
    assert result[0][header[0].index("cut")] == "Any"
    assert result[0][header[0].index("passed_incl")] == 1486
    assert result[0][header[0].index("passed_incl") + 1] == 1486


@pytest.fixture
def config_3():
    return {"All": ["NMuon > 1", {"Any": ["NElectron > 1", "NJet > 1"]}]}


def test_selection_3(config_3, filename):
    selection = filters.build_selection("test_selection_3", config_3)
    infile = uproot.open(filename)["events"]
    mask = selection(infile, is_mc=True)
    assert np.count_nonzero(mask) == 8

    result = selection.results()
    header = selection.results_header()
    assert len(result) == 5
    assert result[0][header[0].index("depth")] == 0
    assert result[0][header[0].index("cut")] == "All"
    assert result[0][header[0].index("passed_incl")] == 8
    assert result[1][header[0].index("depth")] == 1
    assert result[1][header[0].index("cut")] == "NMuon > 1"
    assert result[1][header[0].index("passed_incl")] == 289


@pytest.fixture
def config_jagged_index():
    return dict(reduce=1, formula="Muon_Px > 0.3")


def test_selection_jagged_index(config_jagged_index, filename):
    selection = filters.build_selection("test_selection_jagged", config_jagged_index)
    infile = uproot.open(filename)["events"]
    mask = selection(infile, is_mc=False)
    # Compare to: events->Draw("", "Muon_Px[1] > 0.300")
    assert len(mask) == len(infile)
    assert np.count_nonzero(mask) == 144


@pytest.fixture
def config_jagged_count_nonzero():
    return dict(reduce="any", formula="Muon_Px > 0.3")


def test_selection_jagged_count_nonzero(config_jagged_count_nonzero, filename):
    selection = filters.build_selection("test_selection_jagged", config_jagged_count_nonzero)
    infile = uproot.open(filename)["events"]
    mask = selection(infile, is_mc=False)
    # Compare to: events->Draw("", "Sum$(Muon_Px > 0.300) > 0")
    assert np.count_nonzero(mask) == 2225


def fake_evaluate(variables, expression):
    import numexpr
    return numexpr.evaluate(expression, variables)


class FakeTree(dict):
    def __init__(self):
        super(FakeTree, self).__init__(NMuon=np.linspace(0, 5., 101),
                                       NElectron=np.linspace(0, 10, 101),
                                       NJet=np.linspace(2, -18, 101),
                                       )

    def __len__(self):
        return 101


def test_event_removal_1(config_1, monkeypatch):
    variables = FakeTree()
    monkeypatch.setattr(filters, 'evaluate', fake_evaluate)
    selection = filters.build_selection("test_event_removal", config_1)
    NMuon = variables["NMuon"]

    mask = selection(variables, is_mc=False)
    assert len(mask) == 101
    assert mask.dtype.kind == "b"
    assert len(NMuon[mask]) == 80
    assert NMuon[mask].max() == 5
    assert NMuon[mask].min() == 1.05


def test_event_removal_2(config_2, monkeypatch):
    variables = FakeTree()
    monkeypatch.setattr(filters, 'evaluate', fake_evaluate)
    selection = filters.build_selection("test_event_removal", config_2)

    mask = selection(variables, is_mc=False)

    assert len(mask) == 101
    assert mask.dtype.kind == "b"
    assert np.count_nonzero(mask) == 95
    assert all(mask == [True] * 5 + [False] * 6 + [True] * 90)
    assert variables["NMuon"][mask].min() == 0
    assert variables["NElectron"][mask].min() == 0
    assert variables["NJet"][mask].min() == -18


def test_event_removal_3(config_3, monkeypatch):
    variables = FakeTree()
    monkeypatch.setattr(filters, 'evaluate', fake_evaluate)
    selection = filters.build_selection("test_event_removal", config_3)

    mask = selection(variables, is_mc=False)

    assert len(mask) == 101
    assert mask.dtype.kind == "b"
    assert np.count_nonzero(mask) == 80
    assert all(mask == [False] * 21 + [True] * 80)
    assert variables["NMuon"][mask].min() == 1.05
    assert variables["NElectron"][mask].min() == 2.1
    assert variables["NJet"][mask].max() == -2.2
    assert variables["NJet"][mask].min() == -18
