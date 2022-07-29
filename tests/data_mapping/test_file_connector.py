import pytest
import uproot
from pytest_lazyfixture import lazy_fixture

from fast_carpenter.data_mapping import create_mapping


@pytest.mark.parametrize(
    "tree_input_file, tree_names, var_name",
    [
        (
            lazy_fixture("multi_tree_input_file"),
            [
                "l1CaloTowerEmuTree/L1CaloTowerTree",
                "l1CaloTowerEmuTree/L1CaloTowerTree",
            ],
            "l1CaloTowerEmuTree.L1CaloTowerTree.L1CaloTower.iet",
        ),
        (lazy_fixture("test_input_file"), ["events"], "Muon_Py"),
    ],
)
def test_file_connector(tree_input_file, tree_names, var_name):

    with uproot.open(tree_input_file) as f:
        data_mapping = create_mapping(
            input_file=f,
            treenames=tree_names,
            connector="file",
            methods="uproot4",
        )

        assert data_mapping._connector._treenames == tree_names
        assert data_mapping.num_entries == f[tree_names[0]].num_entries
        assert var_name in data_mapping
        assert "noSuchVar" not in data_mapping
