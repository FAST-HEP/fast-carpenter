import numpy as np
import fast_carpenter.summary.binning_config as mgr
from . import dummy_binning_descriptions as binning


def test_create_one_region():
    cfg = {"_" + k: v for k, v in binning.bins_nmuon.items()}
    _in, _out, _bins, _index = mgr.create_one_dimension("test_create_one_region", **cfg)
    assert _in == "NMuon"
    assert _out == "nmuon"
    assert _index is None
    assert _bins is None


def test_create_one_dimension_aT():
    cfg = {"_" + k: v for k, v in binning.bins_met_px.items()}
    _in, _out, _bins, _index = mgr.create_one_dimension("test_create_one_dimension_aT", **cfg)
    assert _in == "MET_px"
    assert _out == "met_px"
    assert _index is None
    assert isinstance(_bins, np.ndarray)
    assert np.all(_bins[1:-1] == np.linspace(0, 100, 11))
    assert _bins[0] == float("-inf")
    assert _bins[-1] == float("inf")


def test_create_one_dimension_HT():
    cfg = {"_" + k: v for k, v in binning.bins_py.items()}
    _in, _out, _bins, _index = mgr.create_one_dimension("test_create_one_dimension_HT", **cfg)
    assert _in == "Jet_Py"
    assert _out == "py_leadJet"
    assert _index == 0
    assert isinstance(_bins, np.ndarray)
    assert np.all(_bins[1:-1] == [0, 20, 100])
    assert _bins[0] == float("-inf")
    assert _bins[-1] == float("inf")


def test_create_binning_list():
    ins, outs, bins = mgr.create_binning_list("test_create_binning_list", [binning.bins_nmuon, binning.bins_met_px])
    assert ins == ["NMuon", "MET_px"]
    assert outs == ["nmuon", "met_px"]
    assert len(bins) == 2
    assert bins[0] is None


def test_create_weights_list():
    name = "test_create_weights_list"
    weights = mgr.create_weights(name, binning.weight_list)
    assert len(weights) == 1
    assert weights["EventWeight"] == "EventWeight"


def test_create_weights_dict():
    name = "test_create_weights_dict"
    weights = mgr.create_weights(name, binning.weight_dict)
    assert len(weights) == 1
    assert weights["weighted"] == "EventWeight"
