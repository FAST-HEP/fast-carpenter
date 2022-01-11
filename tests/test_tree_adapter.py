import pytest
import fast_carpenter.tree_adapter as tree_adapter
# from fast_carpenter.tree_adapter import TreeToDictAdaptorV0, TreeToDictAdaptorV1, Ranger
import awkward as ak
import numexpr as ne
import numpy as np

###############################################################################
# Uproot3 tests
###############################################################################


@pytest.fixture
def uproot3_adapter(uproot3_tree):
    return tree_adapter.create({"adapter": "uproot3", "tree": uproot3_tree})


def test_uproot3_num_entries(uproot3_tree, uproot3_adapter):
    assert uproot3_adapter.num_entries == uproot3_tree.numentries


def test_uproot3_getitem(uproot3_tree, uproot3_adapter):
    assert ak.all(uproot3_adapter["Muon_Py"] == uproot3_tree["Muon_Py"].array())

###############################################################################
# Uproot4 tests
###############################################################################


@pytest.fixture
def uproot4_adapter(uproot4_tree):
    return tree_adapter.create({"adapter": "uproot4", "tree": uproot4_tree})


@pytest.fixture
def uproot4_ranged_adapter(uproot4_tree, event_range):
    return tree_adapter.create_ranged({"adapter": "uproot4", "tree": uproot4_tree, "start": event_range.start_entry, "stop": event_range.stop_entry})


def test_uproot4_num_entries(uproot4_tree, uproot4_adapter):
    assert uproot4_adapter.num_entries == uproot4_tree.num_entries


def test_uproot4_getitem(uproot4_tree, uproot4_adapter):
    assert ak.all(uproot4_adapter["Muon_Py"] == uproot4_tree["Muon_Py"].array())


def test_uproot4_evaluate(uproot4_tree, uproot4_adapter):
    result = uproot4_adapter.evaluate("Muon_Py * NMuon")
    assert ak.num(result, axis=0) == ak.num(uproot4_tree["Muon_Py"].array(), axis=0)


def test_uproot4_range(uproot4_tree, uproot4_ranged_adapter, event_range):
    assert uproot4_ranged_adapter.num_entries == event_range.entries_in_block


def test_uproot4_add_retrieve(uproot4_tree, uproot4_ranged_adapter):
    muon_px = uproot4_ranged_adapter["Muon_Px"]
    assert len(muon_px) == 100

    muon_py, muon_pz = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    muon_momentum = np.hypot(muon_py, muon_pz)
    uproot4_ranged_adapter.new_variable("Muon_momentum", muon_momentum)

    retrieve_momentum = uproot4_ranged_adapter["Muon_momentum"]
    assert len(retrieve_momentum) == len(muon_momentum)
    assert ak.all(ak.flatten(retrieve_momentum) == ak.flatten(muon_momentum))


def test_overwrite(uproot4_ranged_adapter):
    muon_px = uproot4_ranged_adapter["Muon_Px"]
    assert ("Muon_Px" in uproot4_ranged_adapter)
    with pytest.raises(ValueError) as err:
        uproot4_ranged_adapter.new_variable("Muon_Px", muon_px / muon_px)
    assert "Muon_Px" in str(err)
