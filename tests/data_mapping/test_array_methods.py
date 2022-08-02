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


@pytest.mark.parametrize(
    "methods",
    [
        Uproot3Methods,
        Uproot4Methods,
    ],
)
def test_to_pandas(methods, jagged_list):
    array = methods.awkward_from_iter(jagged_list)
    result = methods.arraydict_to_pandas(dict(a=array, b=array))
    assert (result.keys() == ["a", "b"]).all()


@pytest.mark.parametrize(
    "methods, library, how",
    [
        # (Uproot3Methods,dict),
        (Uproot4Methods, "ak", dict),
        (Uproot4Methods, "ak", list),
        (Uproot4Methods, "ak", tuple),
        (Uproot4Methods, "pandas", None),
    ],
)
def test_array_exporter(methods, library, how, jagged_list):
    array = methods.awkward_from_iter(jagged_list)
    array_dict = {
        "a": array,
        "b": array,
    }
    result = methods.array_exporter(array_dict, library=library, how=how)
    if library == "pandas":
        assert (result.keys() == ["a", "b"]).all()
        return
    assert isinstance(result, how)
    if isinstance(result, dict):
        assert result.keys() == array_dict.keys()
        assert methods.all(result["a"] == array_dict["a"], axis=None)
    if isinstance(result, list):
        assert len(result) == len(array_dict)
        assert methods.all(result[0] == array_dict["a"], axis=None)
    if isinstance(result, tuple):
        assert len(result) == len(array_dict)
        assert methods.all(result[0] == array_dict["a"], axis=None)


@pytest.mark.parametrize(
    "methods",
    [
        # Uproot3Methods,
        Uproot4Methods,
    ],
)
def test_extract_array_dict(methods, jagged_list):
    array = methods.awkward_from_iter(jagged_list)
    array_dict = {
        "a": array,
        "b": array,
    }
    result = methods.extract_array_dict(array_dict, keys=["a"])
    assert list(result.keys()) == ["a"]
    assert methods.all(result["a"] == array_dict["a"], axis=None)


class Extra:
    def __init__(self, ad, extra_variables):
        self.array_dict = ad
        self._extra_variables = extra_variables

    def __getitem__(self, key):
        return self.array_dict[key]


@pytest.mark.parametrize(
    "methods",
    [
        # Uproot3Methods,
        Uproot4Methods,
    ],
)
def test_extract_array_dict_with_extra_variables(methods, jagged_list):
    array = methods.awkward_from_iter(jagged_list)
    array_dict = {
        "a": array,
        "b": array,
    }

    # array_dict._extra_variables = {"c": array}
    result = methods.extract_array_dict(
        Extra(array_dict, {"c": array}), keys=["a", "c"]
    )
    assert list(result.keys()) == ["a", "c"]
    assert methods.all(result["a"] == array_dict["a"], axis=None)
    assert methods.all(result["c"] == array, axis=None)


@pytest.mark.parametrize(
    "methods",
    [
        # Uproot3Methods,
        Uproot4Methods,
    ],
)
def test_arrays(methods, jagged_list):
    array = methods.awkward_from_iter(jagged_list)
    array_dict = {
        "a": array,
        "b": array,
    }
    data = Extra(array_dict, {"c": array})
    result = methods.arrays(
        data,
        ["a", "c"],
    )
    assert list(result.keys()) == ["a", "c"]
    assert methods.all(result["a"] == array_dict["a"], axis=None)
    assert methods.all(result["c"] == array, axis=None)
