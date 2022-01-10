import fast_carpenter.tree_adapter as tree_adapter
# from fast_carpenter.tree_adapter import TreeToDictAdaptorV0, TreeToDictAdaptorV1, Ranger
import awkward as ak
import numexpr as ne

###############################################################################
# Uproot4 tests
###############################################################################


def test_uproot3_num_entries(uproot3_tree):
    adapter = tree_adapter.create({"adapter": "uproot3", "tree": uproot3_tree})
    assert adapter.num_entries == uproot3_tree.numentries


def test_uproot3_getitem(uproot3_tree):
    adapter = tree_adapter.create({"adapter": "uproot3", "tree": uproot3_tree})
    assert ak.all(adapter["Muon_Py"] == uproot3_tree["Muon_Py"].array())

###############################################################################
# Uproot4 tests
###############################################################################


def test_uproot4_num_entries(uproot4_tree):
    adapter = tree_adapter.create({"adapter": "uproot4", "tree": uproot4_tree})
    assert adapter.num_entries == uproot4_tree.num_entries


def test_uproot4_getitem(uproot4_tree):
    adapter = tree_adapter.create({"adapter": "uproot4", "tree": uproot4_tree})
    assert ak.all(adapter["Muon_Py"] == uproot4_tree["Muon_Py"].array())


def test_uproot4_evaluate(uproot4_tree):
    adapter = tree_adapter.create({"adapter": "uproot4", "tree": uproot4_tree})
    result = adapter.evaluate("Muon_Py * NMuon")
    assert ak.num(result, axis=0) == ak.num(uproot4_tree["Muon_Py"].array(), axis=0)


def test_uproot4_range(uproot4_tree, event_range):
    tree = tree_adapter.create_ranged(
        {"adapter": "uproot4", "tree": uproot4_tree,
         "start": event_range.start_entry, "stop": event_range.stop_entry}
    )
    assert tree.num_entries == event_range.entries_in_block


def test_ranger(uproot4_tree, event_range):
    ranger = tree_adapter.create_ranged(
        {"adapter": "uproot4", "tree": uproot4_tree,
         "start": event_range.start_entry, "stop": event_range.stop_entry}
    )
    assert ranger.num_entries == event_range.entries_in_block
