import pytest
import uproot as uproot4

from fast_carpenter.data_mapping import DataMapping  # IndexWithAliases,
from fast_carpenter.data_mapping import (
    FileConnector,
    MultiTreeIndex,
    TokenMapIndex,
    TreeConnector,
    Uproot4Methods,
)


@pytest.fixture
def data_mapping_with_tree(test_input_file):
    f = uproot4.open(test_input_file)
    tree = f["events"]
    methods = Uproot4Methods

    return DataMapping(
        connector=TreeConnector(tree=tree, methods=methods),
        methods=methods,
        indices=[TokenMapIndex()],
    )


@pytest.fixture
def data_mapping_with_file(test_input_file):
    f = uproot4.open(test_input_file)
    methods = Uproot4Methods

    return DataMapping(
        connector=FileConnector(file_handle=f, methods=methods, treenames=["events"]),
        methods=Uproot4Methods,
        indices=[TokenMapIndex()],
    )


@pytest.fixture
def data_mapping_with_multi_tree(multi_tree_input_file):
    f = uproot4.open(multi_tree_input_file)
    methods = Uproot4Methods
    treenames = ["l1CaloTowerEmuTree/L1CaloTowerTree", "l1EventTree/L1EventTree"]

    return DataMapping(
        connector=FileConnector(file_handle=f, methods=methods, treenames=treenames),
        methods=Uproot4Methods,
        indices=[TokenMapIndex(), MultiTreeIndex(prefixes=treenames)],
    )


def test_tree_num_entries(data_mapping_with_tree):
    assert data_mapping_with_tree.num_entries == 4580


def test_file_num_entries(data_mapping_with_file):
    assert data_mapping_with_file.num_entries == 4580


def test_multi_tree_num_entries(data_mapping_with_multi_tree):
    assert data_mapping_with_multi_tree.num_entries == 1853
