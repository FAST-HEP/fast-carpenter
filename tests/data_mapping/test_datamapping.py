import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture

from fast_carpenter.data_mapping.array_methods import Uproot3Methods


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
    if data_mapping_with_tree._methods == Uproot3Methods:
        kwargs = dict(outputtype=dict)
    raw_access = data_mapping_with_tree._connector._tree.arrays(vars, **kwargs)
    for var, data in zip(vars, [px, py, pz]):
        if var not in raw_access.keys():
            var = bytes(var, "utf-8")
        assert data_mapping_with_tree._methods.all(
            data == raw_access[var],
            axis=None,
        )


def test_keys(data_mapping_with_tree):
    all_keys = data_mapping_with_tree.keys()
    assert len(all_keys) == 51


def test_keys_via_tree(data_mapping_with_tree):
    all_keys = data_mapping_with_tree.tree.keys()
    assert len(all_keys) == 51
