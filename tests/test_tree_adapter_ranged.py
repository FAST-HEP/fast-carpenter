import pytest
from pytest_lazyfixture import lazy_fixture

from fast_carpenter import tree_adapter


@pytest.mark.parametrize(
    "tree, start, stop, expected_num_entries",
    [
        (lazy_fixture("input_tree"), 0, -1, lazy_fixture("size_of_test_sample")),
        (lazy_fixture("input_tree"), 0, lazy_fixture("size_of_test_sample"), lazy_fixture("size_of_test_sample")),
        (lazy_fixture("input_tree"), 100, 300, 200),
    ]
)
def test_ranged(tree, start, stop, expected_num_entries):
    test_tree = tree_adapter.create_ranged(
        {
            "adapter": "uproot4", "tree": tree,
            "start": start, "stop": stop,
        }
    )
    assert len(test_tree) == expected_num_entries
