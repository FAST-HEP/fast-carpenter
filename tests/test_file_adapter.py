import pytest
from pytest_lazyfixture import lazy_fixture
import uproot
from fast_carpenter.tree_adapter import FileToDictAdaptor


@pytest.mark.parametrize(
    "tree_input_file, tree_names, var_name",
    [
        (
            lazy_fixture("multi_tree_input_file"),
            [
                "l1CaloTowerEmuTree/L1CaloTowerTree",
                "l1CaloTowerEmuTree/L1CaloTowerTree",
            ],
            "l1CaloTowerEmuTree.L1CaloTowerTree.L1CaloTower.iet"
        ),
        (lazy_fixture("test_input_file"), ["events"], "Muon_Py"),
    ]
)
def test_file_adaptor(tree_input_file, tree_names, var_name):
    with uproot.open(tree_input_file) as f:
        adaptor = FileToDictAdaptor(
            file_handle=f,
            trees=tree_names,
        )
        assert adaptor._trees == tree_names
        assert adaptor.num_entries == f[tree_names[0]].num_entries
        assert var_name in adaptor
        assert "noSuchVar" not in adaptor
