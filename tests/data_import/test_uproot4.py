import pytest
from pytest_lazyfixture import lazy_fixture

from fasthep_carpenter.data_import import get_data_import_plugin

@pytest.mark.parametrize(
    "input_file, tree_names, var_name",
    [
        (
            lazy_fixture("test_multi_tree_input_file"),
            [
                "l1CaloTowerEmuTree/L1CaloTowerTree",
                "l1CaloTowerEmuTree/L1CaloTowerTree",
            ],
            "l1CaloTowerEmuTree/L1CaloTowerTree/L1CaloTower/iet",
        ),
        (lazy_fixture("test_input_file"), ["events"], "events/Muon_Py"),
    ]
)
def test_file_open(input_file, tree_names, var_name):
    reader = get_data_import_plugin("uproot4")
    rootfile = reader.open([input_file])
    assert rootfile is not None
    for name in tree_names:
        assert rootfile[name] is not None
    
    assert rootfile[var_name] is not None
