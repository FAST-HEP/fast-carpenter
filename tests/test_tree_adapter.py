from fast_carpenter.tree_adapter import TreeToDictAdaptorV0, TreeToDictAdaptorV1


def test_uproot3_access(uproot3_tree):
    adapter = TreeToDictAdaptorV0(uproot3_tree)
    assert adapter.num_entries == uproot3_tree.numentries


###############################################################################
# Uproot4 tests
###############################################################################
def test_uproot4_access(uproot4_tree):
    adapter = TreeToDictAdaptorV1(uproot4_tree)
    assert adapter.num_entries == uproot4_tree.num_entries
