import pytest
from pytest_lazyfixture import lazy_fixture
import uproot
from fast_carpenter.tree_adapter import MultiTreeIndex

# MultiTreeIndex tests


# @pytest.mark.parametrize(
#     'path',
#     [
#         ('tree1/s'),
#         ('tree1/folder/something'),
#         (b'tree1/folder/something'),
#         (b'tree1/s'),
#     ]
# )
# def test_normalize_internal_path(path):
#     normPath = MultiTreeIndex.normalize_internal_path(str(path))
#     if type(path) is str:
#         assert '/' in str(path)
#     if type(path) is bytes:
#         assert b'/' in path
#     assert '/' not in str(normPath)


# @pytest.mark.parametrize(
#     'index, provenance, name, value, expected',
#     [
#         ({}, ['path', 'tree'], 'var', 3, {'path.tree.var': 3}),
#         ({}, ['tree'], 'var', 42, {'tree.var': 42}),
#         ({'var': 42}, ['tree'], 'var', 42, {'tree.var': 42, 'var': 42}),
#         ({'tree.var': 42}, ['tree'], 'var', 42, {'tree.var': 42}),
#     ]
# )
# def test_add_to_index(index, provenance, name, value, expected):
#     if provenance:
#         name = '.'.join(provenance + [name])
#     index = MultiTreeIndex.add_to_index(index, name, value)

#     assert index == expected


# @pytest.mark.parametrize(
#     'index, provenance, mapping, expected',
#     [
#         ({}, [], {'var': 3}, {'var': 3}),
#         ({}, ['path', 'tree'], {'var': 3, 'obj': {'et': 42}}, {
#          'path.tree.var': 3, 'path.tree.obj.et': 42,
#          'path.tree.obj': {'et': 42},
#          'path.tree': {'var': 3, 'obj': {'et': 42}},
#          }),
#     ]
# )
# def test_recursive_index(index, provenance, mapping, expected):
#     index = MultiTreeIndex.recursive_index(index, provenance, mapping)
#     assert index == expected


@pytest.mark.parametrize(
    "path, prefix, expected",
    [
        ("tree1.s", None, "tree1/s"),
        ("tree1.folder.something", None, "tree1/folder/something"),
        ("folder.something", "tree1", "tree1/folder/something"),
    ]
)
def test_resolve_index(path, prefix, expected):
    indexer = MultiTreeIndex(prefix)
    index = indexer.resolve_index(path)
    assert index == expected


@pytest.mark.parametrize(
    "tree_input_file, tree_name, var_name",
    [
        (lazy_fixture("multi_tree_input_file"), None, "l1CaloTowerEmuTree.L1CaloTowerTree.L1CaloTower.iet"),
        (lazy_fixture("multi_tree_input_file"), None, "l1EventTree.L1EventTree.Event.lumi"),
        (lazy_fixture("test_input_file"), "events", "Muon_Py"),
    ]
)
def test_resolve_multi_tree(tree_input_file, tree_name, var_name):
    with uproot.open(tree_input_file) as f:
        indexer = MultiTreeIndex(tree_name)
        index = indexer.resolve_index(var_name)
        assert f[index] is not None
        assert len(f[index].array()) > 0

