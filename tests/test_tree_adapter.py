from fast_carpenter.tree_adapter import TreeToDictAdaptorV0, TreeToDictAdaptorV1
import awkward as ak

###############################################################################
# Uproot4 tests
###############################################################################


def test_uproot3_num_entries(uproot3_tree):
    adapter = TreeToDictAdaptorV0(uproot3_tree)
    assert adapter.num_entries == uproot3_tree.numentries


def test_uproot3_getitem(uproot3_tree):
    adapter = TreeToDictAdaptorV0(uproot3_tree)
    assert ak.all(adapter["Muon_Py"] == uproot3_tree["Muon_Py"].array())

###############################################################################
# Uproot4 tests
###############################################################################


def test_uproot4_num_entries(uproot4_tree):
    adapter = TreeToDictAdaptorV1(uproot4_tree)
    assert adapter.num_entries == uproot4_tree.num_entries


def test_uproot4_getitem(uproot4_tree):
    adapter = TreeToDictAdaptorV1(uproot4_tree)
    print(uproot4_tree, type(uproot4_tree), dir(uproot4_tree))
    assert ak.all(adapter["Muon_Py"] == uproot4_tree["Muon_Py"].array())
