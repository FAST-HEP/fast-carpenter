import pytest
import awkward as ak
import numpy as np

from fast_carpenter.testing import FakeBEEvent
import fast_carpenter.tree_adapter as tree_adapter
from fast_carpenter.tree_adapter import ArrayMethods


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
    return tree_adapter.create_ranged(
        {
            "adapter": "uproot4",
            "tree": uproot4_tree,
            "start": event_range.start_entry,
            "stop": event_range.stop_entry
        }
    )


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


def test_to_pandas(full_wrapped_tree):
    chunk = FakeBEEvent(full_wrapped_tree, "mc")
    inputs = ['Electron_Px', 'Electron_Py', 'EventWeight']
    df = ArrayMethods.to_pandas(chunk.tree, inputs)
    assert list(df.keys()) == inputs


def test_arraydict_to_pandas_with_new_variable(uproot4_ranged_adapter):
    muon_py, muon_pz = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    muon_momentum = np.hypot(muon_py, muon_pz)
    uproot4_ranged_adapter.new_variable("Muon_momentum", muon_momentum)

    inputs = ['Muon_Py', 'Muon_Pz', 'Muon_momentum']
    arrays = {
        'Muon_Py': muon_py,
        'Muon_Pz': muon_pz,
        'Muon_momentum': muon_momentum,
    }
    df = ArrayMethods.arraydict_to_pandas(arrays)

    assert list(df.keys()) == inputs
    assert len(df) == ak.count_nonzero(muon_py)


def test_to_pandas_with_new_variable(uproot4_ranged_adapter):
    muon_py, muon_pz = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    muon_momentum = np.hypot(muon_py, muon_pz)
    uproot4_ranged_adapter.new_variable("Muon_momentum", muon_momentum)

    inputs = ['Muon_Py', 'Muon_Pz', 'Muon_momentum']
    df = ArrayMethods.to_pandas(uproot4_ranged_adapter, inputs)

    assert list(df.keys()) == inputs
    assert len(df) == ak.count_nonzero(muon_py)


def test_arrays_to_tuple(uproot4_ranged_adapter):
    muon_py, muon_pz = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    muon_momentum = np.hypot(muon_py, muon_pz)
    uproot4_ranged_adapter.new_variable("Muon_momentum", muon_momentum)
    _, _, muon_momentum_new = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz", "Muon_momentum"], how=tuple)
    assert ak.all(muon_momentum_new == muon_momentum)


def test_arrays_to_dict(uproot4_ranged_adapter):
    muon_py, muon_pz = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    muon_momentum = np.hypot(muon_py, muon_pz)
    uproot4_ranged_adapter.new_variable("Muon_momentum", muon_momentum)
    array_dict = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz", "Muon_momentum"], how=dict)
    assert ak.all(array_dict["Muon_momentum"] == muon_momentum)


def test_arrays_as_np_lists(uproot4_ranged_adapter):
    muon_py, muon_pz = uproot4_ranged_adapter.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    muon_momentum = np.hypot(muon_py, muon_pz)
    uproot4_ranged_adapter.new_variable("Muon_momentum", muon_momentum)
    array_dict = uproot4_ranged_adapter.arrays_as_np_lists(["Muon_Py", "Muon_Pz", "Muon_momentum"], how=dict)
    assert ak.all(array_dict["Muon_momentum"] == muon_momentum)
