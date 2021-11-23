import pytest
import six
import numpy as np
import uproot3
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
    assert isinstance(selection._wrapped_selection, filters.SingleCut)
    assert isinstance(selection.selection, six.string_types)


def test_selection_1(config_1, filename):
    selection = filters.build_selection("test_selection_1", config_1)
    infile = uproot3.open(filename)["events"]
    mask = selection(infile, is_mc=False)
    assert np.count_nonzero(mask) == 289

    columns = selection.columns
    values = selection.values
    index = selection.index_values
    assert len(values) == 1
    assert len(index) == 1
    assert index[0][0] == "0"
    assert index[0][1] == 0
    assert index[0][2] == "NMuon > 1"
    assert values[0][columns[0].index("passed_incl")] == 289
    assert values[0][columns[0].index("passed_only_cut")] == 289
    assert values[0][columns[0].index("totals_incl")] == 4580

    df = selection.to_dataframe()
    assert len(df) == 1
    assert all(df.index.get_level_values("depth") == [0])
    assert all(df.index.get_level_values("cut") == ["NMuon > 1"])
    row = ("0", 0, "NMuon > 1")
    assert df.loc[row, ("passed_incl", "unweighted")] == 289
    assert df.loc[row, ("passed_only_cut", "unweighted")] == 289
    assert df.loc[row, ("totals_incl", "unweighted")] == 4580


@pytest.fixture
def config_2():
    return {"Any": ["NMuon > 1", "NElectron > 1", "NJet > 1"]}


def test_selection_2_weights(config_2, filename):
    selection = filters.build_selection("test_selection_1",
                                        config_2, weights=["EventWeight"])
    infile = uproot3.open(filename)["events"]
    mask = selection(infile, is_mc=True)
    assert np.count_nonzero(mask) == 1486

    columns = selection.columns
    assert len(columns) == 2
    assert len(columns[0]) == 6

    values = selection.values
    index = selection.index_values
    assert len(values) == 4
    assert len(index) == 4
    assert index[0] == ("0", 0, "Any")
    assert values[0][columns[0].index("passed_incl")] == 1486
    assert values[0][columns[0].index("passed_incl") + 1] == np.sum(infile.array("EventWeight")[mask])


def test_selection_2_weights_data(config_2, filename):
    selection = filters.build_selection("test_selection_1",
                                        config_2, weights=["EventWeight"])
    infile = uproot3.open(filename)["events"]
    mask = selection(infile, is_mc=False)
    assert np.count_nonzero(mask) == 1486

    columns = selection.columns
    assert len(columns) == 2
    assert len(columns[0]) == 6

    index = selection.index_values
    values = selection.values

    assert len(values) == 4
    assert len(index) == 4
    assert index[0] == ("0", 0, "Any")
    assert index[1] == ("0,0", 1, "NMuon > 1")
    assert values[0][columns[0].index("passed_incl")] == 1486
    assert values[0][columns[0].index("passed_incl") + 1] == 1486


@pytest.fixture
def config_3():
    return {"All": ["NMuon > 1", {"Any": ["NElectron > 1", "NJet > 1"]}]}


def test_selection_3(config_3, filename):
    selection = filters.build_selection("test_selection_3", config_3)
    infile = uproot3.open(filename)["events"]
    mask = selection(infile, is_mc=True)
    assert np.count_nonzero(mask) == 8

    index = selection.index_values
    values = selection.values
    columns = selection.columns
    assert len(values) == 5
    assert index[0] == ("0", 0, "All")
    assert index[1] == ("0,0", 1, "NMuon > 1")
    assert values[0][columns[0].index("passed_incl")] == 8
    assert values[1][columns[0].index("passed_incl")] == 289


@pytest.fixture
def config_jagged_index():
    return dict(reduce=1, formula="Muon_Px > 0.3")


def test_selection_jagged_index(config_jagged_index, filename):
    selection = filters.build_selection("test_selection_jagged", config_jagged_index)
    infile = uproot3.open(filename)["events"]
    mask = selection(infile, is_mc=False)
    # Compare to: events->Draw("", "Muon_Px[1] > 0.300")
    assert len(mask) == len(infile)
    assert np.count_nonzero(mask) == 144


@pytest.fixture
def config_jagged_count_nonzero():
    return dict(reduce="any", formula="Muon_Px > 0.3")


def test_selection_jagged_count_nonzero(config_jagged_count_nonzero, filename):
    selection = filters.build_selection("test_selection_jagged", config_jagged_count_nonzero)
    infile = uproot3.open(filename)["events"]
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
    nmuon = variables["NMuon"]

    mask = selection(variables, is_mc=False)
    assert len(mask) == 101
    assert mask.dtype.kind == "b"
    assert len(nmuon[mask]) == 80
    assert nmuon[mask].max() == 5
    assert nmuon[mask].min() == 1.05


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
