import pytest
import numpy as np
import fast_carpenter.summary as summary


@pytest.fixture
def bins_alphaT():
    return {"in": "AlphaT", "out": "alphaT", "bins": dict(nbins=10, low=0, high=2.5)}


@pytest.fixture
def bins_pt():
    return {"in": "jet_pt", "out": "pt_leadJet", "bins": dict(edges=[0, 20., 100.], overflow=True), "index": 0}


@pytest.fixture
def bins_region():
    return {"in": "REGION", "out": "region"}


@pytest.fixture
def weight_list():
    return ["weight"]


@pytest.fixture
def weight_dict():
    return dict(weighted="weight")


def test__create_one_region(bins_region):
    cfg = {"_" + k: v for k, v in bins_region.items()}
    _in, _out, _bins, _index = summary._create_one_dimension("test__create_one_region", **cfg)
    assert _in == "REGION"
    assert _out == "region"
    assert _index is None
    assert _bins is None


def test__create_one_dimension_aT(bins_alphaT):
    cfg = {"_" + k: v for k, v in bins_alphaT.items()}
    _in, _out, _bins, _index = summary._create_one_dimension("test__create_one_dimension_aT", **cfg)
    assert _in == "AlphaT"
    assert _out == "alphaT"
    assert _index is None
    assert isinstance(_bins, np.ndarray)
    assert np.all(_bins[1:-1] == np.linspace(0, 2.5, 11))
    assert _bins[0] == float("-inf")
    assert _bins[-1] == float("inf")


def test__create_one_dimension_HT(bins_pt):
    cfg = {"_" + k: v for k, v in bins_pt.items()}
    _in, _out, _bins, _index = summary._create_one_dimension("test__create_one_dimension_HT", **cfg)
    assert _in == "jet_pt"
    assert _out == "pt_leadJet"
    assert _index == 0
    assert isinstance(_bins, np.ndarray)
    assert np.all(_bins[1:-1] == [0, 20, 100])
    assert _bins[0] == float("-inf")
    assert _bins[-1] == float("inf")


def test__create_binning_list(bins_region, bins_alphaT):
    ins, outs, binning = summary._create_binning_list("test__create_binning_list", [bins_region, bins_alphaT])
    assert ins == ["REGION", "AlphaT"]
    assert outs == ["region", "alphaT"]
    assert len(binning) == 2
    assert binning[0] is None


def test__create_weights_list(weight_list):
    name = "test__create_weights_list"
    weights = summary._create_weights(name, weight_list)
    assert len(weights) == 1
    assert weights["weight"] == "weight"


def test__create_weights_dict(weight_dict):
    name = "test__create_weights_dict"
    weights = summary._create_weights(name, weight_dict)
    assert len(weights) == 1
    # assert isinstance(weights["none"], WeightCalculatorConst)
    assert weights["weighted"] == "weight"


@pytest.fixture
def config_1(bins_alphaT, bins_pt, weight_list):
    return dict(binning=[bins_alphaT, bins_pt], weights=weight_list)


@pytest.fixture
def config_2(bins_alphaT, bins_pt, bins_region, weight_dict):
    return dict(binning=[bins_pt, bins_alphaT, bins_region], weights=weight_dict)


@pytest.fixture
def binned_df_1(tmpdir, config_1):
    return summary.BinnedDataframe("binned_df_1", out_dir="somewhere", **config_1)


def test_BinnedDataframe(binned_df_1, tmpdir):
    assert binned_df_1.name == "binned_df_1"
    assert len(binned_df_1._binnings) == 2
    # bin length for alphaT: nbins, plus 1 for edge, plus 2 for +-inf
    assert binned_df_1._bin_dims[0] == "AlphaT"
    assert len(binned_df_1._binnings[0]) == 10 + 1 + 2
    assert len(binned_df_1._weights) == 1
