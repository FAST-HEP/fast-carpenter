import awkward as ak
import numpy as np


def test_broadcast_arrays(uproot4_tree):
    output = ak.broadcast_arrays(5, [1, 2, 3, 4, 5])
    assert ak.num(output[0], axis=0) == 5
    assert ak.num(output[1], axis=0) == 5

    output = ak.broadcast_arrays(uproot4_tree["Muon_Py"].array(), uproot4_tree["NMuon"].array())
    assert ak.num(output[0], axis=0) == ak.num(output[1], axis=0)
    for py, n in zip(output[0], output[1]):
        assert ak.num(py, axis=0) == ak.num(n, axis=0)

    arrays = uproot4_tree.arrays(["NMuon", "Muon_Py"], how=tuple)
    output = ak.broadcast_arrays(*arrays, right_broadcast=True, left_broadcast=True)
    assert ak.num(output[0], axis=0) == ak.num(output[1], axis=0)
    for n, py in zip(output[0], output[1]):
        assert ak.num(py, axis=0) == ak.num(n, axis=0)


def test_pandas(uproot4_tree):
    muon_py, muon_pz = uproot4_tree.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    addon = {"Muon_momentum": np.hypot(muon_py, muon_pz)}

    df = uproot4_tree.arrays(["Muon_Py", "Muon_Pz"], library="pd")
    addon_df = ak.to_pandas(addon)
    assert len(df) == ak.count_nonzero(muon_py)
    df = df.merge(addon_df, left_index=True, right_index=True)

    assert len(df) == ak.count_nonzero(muon_py)
    assert all(df.keys() == ["Muon_Py", "Muon_Pz", "Muon_momentum"])


def test_pandas_mixed_vars(uproot4_tree):
    muon_py, muon_pz, n_muon = uproot4_tree.arrays(["Muon_Py", "Muon_Pz", "NMuon"], how=tuple)
    addon = {"Muon_momentum": np.hypot(muon_py, muon_pz)}

    df = uproot4_tree.arrays(["Muon_Py", "Muon_Pz", "NMuon"], library="pd")
    addon_df = ak.to_pandas(addon)
    assert len(df) == ak.count_nonzero(muon_py)
    df = df.merge(addon_df, left_index=True, right_index=True)

    assert len(df) == ak.count_nonzero(muon_py)
    assert all(df.keys() == ["Muon_Py", "Muon_Pz", "NMuon", "Muon_momentum"])
