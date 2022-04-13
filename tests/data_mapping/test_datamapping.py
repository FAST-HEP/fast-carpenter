import numpy as np
import pytest
from pytest_lazyfixture import lazy_fixture


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
