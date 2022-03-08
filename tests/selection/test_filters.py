import pytest
import six
import numpy as np
import fast_carpenter.selection.filters as filters
from fast_carpenter.tree_adapter import ArrayMethods
from fast_carpenter.testing import FakeTree


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


def test_selection_1(config_1, full_wrapped_tree):
    selection = filters.build_selection("test_selection_1", config_1)
    mask = selection(full_wrapped_tree, is_mc=False)
    assert np.count_nonzero(ArrayMethods.only_valid_entries(mask)) == 289

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


def test_selection_2_weights(config_2, full_wrapped_tree):
    selection = filters.build_selection("test_selection_1",
                                        config_2, weights=["EventWeight"])
    mask = selection(full_wrapped_tree, is_mc=True)
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

    weight_sum = ArrayMethods.sum(full_wrapped_tree["EventWeight"][mask], axis=-1)
    assert values[0][columns[0].index("passed_incl") + 1] == pytest.approx(weight_sum, 1e-4)


def test_selection_2_weights_data(config_2, full_wrapped_tree):
    selection = filters.build_selection("test_selection_1",
                                        config_2, weights=["EventWeight"])
    mask = selection(full_wrapped_tree, is_mc=False)
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


def test_selection_3(config_3, full_wrapped_tree):
    selection = filters.build_selection("test_selection_3", config_3)
    mask = selection(full_wrapped_tree, is_mc=True)
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


def test_selection_jagged_index(config_jagged_index, full_wrapped_tree):
    selection = filters.build_selection("test_selection_jagged", config_jagged_index)
    mask = selection(full_wrapped_tree, is_mc=False)
    # Compare to: events->Draw("", "Muon_Px[1] > 0.300")
    assert len(mask) == len(full_wrapped_tree)
    assert np.count_nonzero(mask) == 144


@pytest.fixture
def config_jagged_count_nonzero():
    return dict(reduce="any", formula="Muon_Px > 0.3")


def test_selection_jagged_count_nonzero(config_jagged_count_nonzero, full_wrapped_tree):
    selection = filters.build_selection("test_selection_jagged", config_jagged_count_nonzero)
    mask = selection(full_wrapped_tree, is_mc=False)
    # Compare to: events->Draw("", "Sum$(Muon_Px > 0.300) > 0")
    assert np.count_nonzero(mask) == 2225


def fake_evaluate(variables, expression):
    import numexpr
    return numexpr.evaluate(expression, variables)


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
