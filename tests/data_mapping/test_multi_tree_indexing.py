import pytest
from pytest_lazyfixture import lazy_fixture
import uproot
# from fast_carpenter.tree_adapter import MultiTreeIndex
from fasthep_carpenter.data_mapping import MultiTreeIndex


@pytest.mark.parametrize(
    "path, prefix, expected",
    [
        (
            "tree1.s",
            "tree1",
            "tree1/s",
        ),
        (
            "tree1.folder.something",
            "tree1/folder/something",
            "tree1/folder/something",
        ),
        (
            "folder.something",
            "tree1",
            "tree1/folder/something",
        ),
    ]
)
def test_resolve_index(path, prefix, expected):
    indexer = MultiTreeIndex([prefix])
    index = indexer.resolve_index(path)
    assert index == expected


@pytest.mark.parametrize(
    "tree_input_file, tree_name, var_name, expected_index",
    [
        (
            lazy_fixture("test_multi_tree_input_file"),
            "l1CaloTowerEmuTree/L1CaloTowerTree",
            "l1CaloTowerEmuTree.L1CaloTowerTree.L1CaloTower.iet",
            "l1CaloTowerEmuTree/L1CaloTowerTree/L1CaloTower/iet",
        ),
        (
            lazy_fixture("test_multi_tree_input_file"),
            "l1EventTree/L1EventTree",
            "l1EventTree.L1EventTree.Event.lumi",
            "l1EventTree/L1EventTree/Event/lumi",
        ),
        (
            lazy_fixture("test_input_file"),
            "events",
            "Muon_Py",
            "events/Muon_Py",
        ),
    ]
)
def test_resolve_multi_tree(tree_input_file, tree_name, var_name, expected_index):
    with uproot.open(tree_input_file) as f:
        indexer = MultiTreeIndex([tree_name])
        index = indexer.resolve_index(var_name)
        assert index == expected_index
        assert f[index] is not None
        assert len(f[index].array()) > 0
