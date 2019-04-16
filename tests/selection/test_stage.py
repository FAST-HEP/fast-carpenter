import pytest
import fast_carpenter.selection.stage as stage
from fast_carpenter.masked_tree import MaskedUprootTree
import fast_carpenter.selection.filters as filters
from ..conftest import FakeBEEvent


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
def cutflow_1(tmpdir):
    return stage.CutFlow("cutflow_1", str(tmpdir), selection="NMuon > 1", weights="NElectron")


def test_cutflow_1(cutflow_1):
    assert cutflow_1._weights == {"NElectron": "NElectron"}
    assert isinstance(cutflow_1.selection, filters.OuterCounterIncrementer)
    assert isinstance(cutflow_1.selection.selection, filters.SingleCut)


def test_cutflow_1_executes_mc(cutflow_1, infile, full_event_range, tmpdir):
    chunk = FakeBEEvent(MaskedUprootTree(infile, event_ranger=full_event_range), "mc")
    cutflow_1.event(chunk)

    assert len(chunk.tree) == 289

    collector = cutflow_1.collector()
    assert collector.filename == str(tmpdir / "cuts_cutflow_1-NElectron.csv")

    dataset_readers_list = (("test_mc", (cutflow_1, )), )
    output = collector._merge_data(dataset_readers_list)
    assert len(output) == 1
    assert all(output[("passed_only_cut", "unweighted")] == [289])
    assert all(output[("passed_incl", "unweighted")] == [289])
    assert all(output[("totals_incl", "unweighted")] == [4580])



def test_cutflow_1_executes_data(cutflow_1, infile, full_event_range, tmpdir):
    chunk = FakeBEEvent(MaskedUprootTree(infile, event_ranger=full_event_range), "data")
    cutflow_1.event(chunk)

    assert len(chunk.tree) == 289

    collector = cutflow_1.collector()
    assert collector.filename == str(tmpdir / "cuts_cutflow_1-NElectron.csv")


@pytest.fixture
def select_2(tmpdir):
    select = {"All": ["NMuon > 1",
                      {"Any": ["NElectron > 1", "NJet > 1"]},
                      {"reduce": 1, "formula": "Muon_Px > 0.3"}]}
    return select


def test_cutflow_2_collect(select_2, infile, full_event_range, tmpdir):
    chunk_data = FakeBEEvent(MaskedUprootTree(infile, event_ranger=full_event_range), "data")
    chunk_mc = FakeBEEvent(MaskedUprootTree(infile, event_ranger=full_event_range), "mc")

    cutflow_a = stage.CutFlow("cutflow_a", str(tmpdir), selection=select_2, weights="EventWeight")
    cutflow_b = stage.CutFlow("cutflow_b", str(tmpdir), selection=select_2, weights="EventWeight")
    cutflow_a.event(chunk_mc)
    chunk_mc.tree.reset_mask()
    cutflow_a.event(chunk_mc)

    cutflow_b.event(chunk_data)
    chunk_data.tree.reset_mask()
    cutflow_b.event(chunk_data)

    collector = cutflow_a.collector()
    dataset_readers_list = (("test_mc", (cutflow_a,)), ("test_data", (cutflow_b,)),)
    output = collector._merge_data(dataset_readers_list)
    assert len(output) == 12
    data = output.xs("test_data", level="dataset", axis="rows")
    data_weighted = data.xs("EventWeight", level=1, axis="columns")
    data_unweighted = data.xs("unweighted", level=1, axis="columns")
    assert all(data_weighted == data_unweighted)
    mc = output.xs("test_mc", level="dataset", axis="rows")
    mc_unweighted = mc.xs("unweighted", level=1, axis="columns")
    assert all(mc_unweighted == data_unweighted)
    assert output.loc[("test_data", 0, "All"), ("totals_incl", "unweighted")] == 4580 * 2
    assert output.loc[("test_data", 0, "All"), ("totals_incl", "EventWeight")] == 4580 * 2
    assert output.loc[("test_mc", 0, "All"), ("totals_incl", "unweighted")] == 4580 * 2
    assert output.loc[("test_data", 1, "NMuon > 1"), ("passed_only_cut", "unweighted")] == 289 * 2
    assert output.loc[("test_mc", 1, "NMuon > 1"), ("passed_only_cut", "unweighted")] == 289 * 2
