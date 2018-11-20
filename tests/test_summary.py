import pytest
import numpy as np
import fast_carpenter.summary as summary
from .conftest import FakeBEEvent


@pytest.fixture
def bins_met_px():
    return {"in": "MET_px", "out": "met_px", "bins": dict(nbins=10, low=0, high=100)}


@pytest.fixture
def bins_py():
    return {"in": "Jet_Py", "out": "py_leadJet", "bins": dict(edges=[0, 20., 100.], overflow=True), "index": 0}


@pytest.fixture
def bins_nmuon():
    return {"in": "NMuon", "out": "nmuon"}


@pytest.fixture
def weight_list():
    return ["EventWeight"]


@pytest.fixture
def weight_dict():
    return dict(weighted="EventWeight")


def test__create_one_region(bins_nmuon):
    cfg = {"_" + k: v for k, v in bins_nmuon.items()}
    _in, _out, _bins, _index = summary._create_one_dimension("test__create_one_region", **cfg)
    assert _in == "NMuon"
    assert _out == "nmuon"
    assert _index is None
    assert _bins is None


def test__create_one_dimension_aT(bins_met_px):
    cfg = {"_" + k: v for k, v in bins_met_px.items()}
    _in, _out, _bins, _index = summary._create_one_dimension("test__create_one_dimension_aT", **cfg)
    assert _in == "MET_px"
    assert _out == "met_px"
    assert _index is None
    assert isinstance(_bins, np.ndarray)
    assert np.all(_bins[1:-1] == np.linspace(0, 100, 11))
    assert _bins[0] == float("-inf")
    assert _bins[-1] == float("inf")


def test__create_one_dimension_HT(bins_py):
    cfg = {"_" + k: v for k, v in bins_py.items()}
    _in, _out, _bins, _index = summary._create_one_dimension("test__create_one_dimension_HT", **cfg)
    assert _in == "Jet_Py"
    assert _out == "py_leadJet"
    assert _index == 0
    assert isinstance(_bins, np.ndarray)
    assert np.all(_bins[1:-1] == [0, 20, 100])
    assert _bins[0] == float("-inf")
    assert _bins[-1] == float("inf")


def test__create_binning_list(bins_nmuon, bins_met_px):
    ins, outs, binning = summary._create_binning_list("test__create_binning_list", [bins_nmuon, bins_met_px])
    assert ins == ["NMuon", "MET_px"]
    assert outs == ["nmuon", "met_px"]
    assert len(binning) == 2
    assert binning[0] is None


def test__create_weights_list(weight_list):
    name = "test__create_weights_list"
    weights = summary._create_weights(name, weight_list)
    assert len(weights) == 1
    assert weights["EventWeight"] == "EventWeight"


def test__create_weights_dict(weight_dict):
    name = "test__create_weights_dict"
    weights = summary._create_weights(name, weight_dict)
    assert len(weights) == 1
    assert weights["weighted"] == "EventWeight"


@pytest.fixture
def config_1(bins_met_px, bins_py, weight_list):
    return dict(binning=[bins_met_px, bins_py], weights=weight_list)


@pytest.fixture
def config_2(bins_met_px, bins_py, bins_nmuon, weight_dict):
    return dict(binning=[bins_py, bins_met_px, bins_nmuon], weights=weight_dict)


@pytest.fixture
def binned_df_1(tmpdir, config_1):
    return summary.BinnedDataframe("binned_df_1", out_dir="somewhere", **config_1)


def test_BinnedDataframe(binned_df_1, tmpdir):
    assert binned_df_1.name == "binned_df_1"
    assert len(binned_df_1._binnings) == 2
    # bin length for met_px: nbins, plus 1 for edge, plus 2 for +-inf
    assert binned_df_1._bin_dims[0] == "MET_px"
    assert len(binned_df_1._binnings[0]) == 10 + 1 + 2
    assert len(binned_df_1._weights) == 1


@pytest.fixture
def binned_df_2(tmpdir, config_2):
    return summary.BinnedDataframe("binned_df_2", out_dir="somewhere", **config_2)


def test_BinnedDataframe_run_mc(binned_df_1, tmpdir, infile):
    chunk = FakeBEEvent(infile, "mc")
    collector = binned_df_1.collector()

    binned_df_1.event(chunk)
    dataset_readers_list = (("test_dataset", (binned_df_1,)),)
    results = collector._prepare_output(dataset_readers_list)

    totals = results.sum()
    # Based on: events->Draw("Jet_Py", "", "goff")
    assert totals["n"] == 4616

    # Based on:
    # events->Draw("EventWeight * (Jet_Py/Jet_Py)>>htemp", "", "goff")
    # htemp->GetMean() * htemp->GetEntries()
    assert totals["EventWeight:sumw"] == pytest.approx(231.91339)


def test_BinnedDataframe_run_data(binned_df_2, tmpdir, infile):
    chunk = FakeBEEvent(infile, "data")
    binned_df_2.event(chunk)


def test_BinnedDataframe_run_twice(binned_df_1, tmpdir, infile):
    chunk = FakeBEEvent(infile, "mc")
    collector = binned_df_1.collector()

    binned_df_1.event(chunk)
    binned_df_1.event(chunk)

    dataset_readers_list = (("test_dataset", (binned_df_1,)),)
    results = collector._prepare_output(dataset_readers_list)

    totals = results.sum()
    # Based on: events->Draw("Jet_Py", "", "goff")
    assert totals["n"] == 4616 * 2

    # Based on:
    # events->Draw("EventWeight * (Jet_Py/Jet_Py)>>htemp", "", "goff")
    # htemp->GetMean() * htemp->GetEntries()
    assert totals["EventWeight:sumw"] == pytest.approx(231.91339 * 2)
