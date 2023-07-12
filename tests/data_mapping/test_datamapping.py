import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture

from fasthep_carpenter.data_mapping import Uproot4Methods


@pytest.mark.parametrize(
    "data_mapping, expected_num_entries",
    [
        (lazy_fixture("data_mapping_with_tree"), 4580),
        (lazy_fixture("data_mapping_with_file"), 4580),
        (lazy_fixture("data_mapping_with_multi_tree"), 1853),
    ],
)
def test_num_entries(data_mapping, expected_num_entries):
    assert data_mapping.num_entries == expected_num_entries


@pytest.mark.parametrize(
    "data_mapping, new_variable_name",
    [
        (lazy_fixture("data_mapping_with_tree"), "Muon_momentum"),
        (lazy_fixture("data_mapping_with_file"), "Muon_momentum"),
        (lazy_fixture("data_mapping_with_multi_tree"), "something"),
    ],
)
def test_new_variable(data_mapping, new_variable_name):
    value = np.ones(data_mapping.num_entries)
    data_mapping.add_variable(new_variable_name, value)
    assert new_variable_name in data_mapping
    assert (data_mapping[new_variable_name] == value).all()


def test_overwrite_existing_variable(data_mapping_with_tree):
    var_name = "Muon_Px"
    assert var_name in data_mapping_with_tree
    # no error by default
    data_mapping_with_tree.add_variable(
        var_name, np.ones(data_mapping_with_tree.num_entries)
    )

    # error if fail_on_overwrite is True
    data_mapping_with_tree._config.fail_on_overwrite = True

    with pytest.raises(ValueError) as err:
        data_mapping_with_tree.add_variable(
            var_name, np.ones(data_mapping_with_tree.num_entries)
        )
    assert var_name in str(err)


def test_getitem(data_mapping_with_tree):
    """DataMapping should map to arrays instead of the tree branches."""
    var_name = "Muon_Px"
    assert var_name in data_mapping_with_tree
    raw_access = data_mapping_with_tree._connector._tree[var_name].array()
    assert data_mapping_with_tree._methods.all(
        data_mapping_with_tree[var_name] == raw_access,
        axis=None,
    )


def test_arrays(data_mapping_with_tree):
    vars = ["Muon_Px", "Muon_Py", "Muon_Pz"]
    for var in vars:
        assert var in data_mapping_with_tree
    px, py, pz = data_mapping_with_tree[vars]
    kwargs = dict(how=dict)
    raw_access = data_mapping_with_tree.arrays(vars, **kwargs)
    for var, data in zip(vars, [px, py, pz]):
        if var not in raw_access.keys():
            var = bytes(var, "utf-8")
        assert data_mapping_with_tree._methods.all(
            data == raw_access[var],
            axis=None,
        )
    
def test_uproot3_parameter_conversion(data_mapping_with_tree):
    """Uproot4 does not support the `outputtype` parameter, so it should be
    converted to `how` within the uproot4 connector.
    """
    assert data_mapping_with_tree._connector._methods == Uproot4Methods
    vars = ["Muon_Px", "Muon_Py", "Muon_Pz"]
    data_mapping_with_tree.arrays(vars, outputtype=dict)


def test_keys(data_mapping_with_tree):
    all_keys = data_mapping_with_tree.keys()
    assert len(all_keys) == 51


def test_keys_via_tree(data_mapping_with_tree):
    all_keys = data_mapping_with_tree.tree.keys()
    assert len(all_keys) == 51


def test_keys_with_extra_variables(data_mapping_with_tree):
    """Legacy test"""
    data_mapping_with_tree.add_variable(
        "extra_variable", np.ones(data_mapping_with_tree.num_entries)
    )
    all_keys = data_mapping_with_tree.keys()
    assert len(all_keys) == 52


def test_config(data_mapping_with_tree):
    """Legacy test"""
    config = data_mapping_with_tree.config
    dataset = config.dataset
    assert dataset.eventtype == "mc"
