import pytest

from fast_carpenter.data_mapping import Uproot3Methods, Uproot4Methods


@pytest.fixture
def jagged_list():
    return [[1, 2, 3], [], [4, 5], [6], [7, 8, 9, 10]]


@pytest.mark.parametrize(
    "methods",
    [
        Uproot3Methods,
        Uproot4Methods,
    ],
)
def test_all(methods, jagged_list):
    array = methods.awkward_from_iter(jagged_list)
    assert methods.all(array, axis=None)


@pytest.mark.parametrize(
    "methods",
    [
        Uproot3Methods,
        Uproot4Methods,
    ],
)
def test_evaluate(methods, jagged_list):
    array = methods.awkward_from_iter(jagged_list)
    array2 = array
    result = methods.evaluate(dict(a=array, b=array2), "a + b")
    assert methods.all(result == array + array2, axis=None)
