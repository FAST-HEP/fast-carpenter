from collections.abc import Iterable
from typing import Any, Dict, List, Protocol

import awkward as ak
import awkward0 as ak0


class ArrayMethodsProtocol(Protocol):
    @staticmethod
    def all(data: Any, **kwargs) -> bool:
        raise NotImplementedError()

    @staticmethod
    def arraydict_to_pandas(arraydict: Dict[str, Any]):
        """
        Converts a dictionary of arrays to a pandas DataFrame.
        """
        raise NotImplementedError()

    @staticmethod
    def awkward_from_iter(data: Iterable) -> Any:
        raise NotImplementedError()

    @staticmethod
    def extract_array_dict(data: Any, keys: List[str]) -> Dict[str, Any]:
        """
        Returns a dictionary of arrays for the given keys.
        """
        raise NotImplementedError()

    @staticmethod
    def array_exporter(dict_of_arrays: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def array_from_tree(tree: Any, key: str) -> Any:
        raise NotImplementedError()

    @staticmethod
    def arrays(data: Any, expressions: str, *args, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def array(data: Any, key: str) -> Any:
        raise NotImplementedError()

    @staticmethod
    def contains(data: Any, key: str) -> bool:
        raise NotImplementedError()

    @staticmethod
    def evaluate(data: Any, expression: str, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def counts(data: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def pad(data: Any, length: int, **kwargs: Dict[str, Any]) -> Any:
        raise NotImplementedError()

    @staticmethod
    def flatten(data: Any, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def num_entries(array: Any) -> int:
        raise NotImplementedError()

    @staticmethod
    def sum(data: Any, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def prod(array: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def any(array: Any, **kwargs) -> bool:
        raise NotImplementedError()

    @staticmethod
    def count_nonzero(array: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def max(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def min(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def argmax(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def argmin(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def count_zero(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def dtype(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def is_bool(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def arrays_as_np_array(data, array_names, **kwargs):
        """
        Takes input data and converts it to an array of numpy arrays.
        e.g. arrays_as_np_array(data, ["x", "y"])
        results in
        array(<array of x>, <array of y>)
        """
        raise NotImplementedError()

    @staticmethod
    def to_pandas(data, keys):
        raise NotImplementedError()

    @staticmethod
    def valid_entry_mask(data: Any) -> Any:
        raise NotImplementedError()

    @staticmethod
    def only_valid_entries(data: Any) -> Any:
        raise NotImplementedError()

    @staticmethod
    def filtered_len(data: Any) -> int:
        raise NotImplementedError()

    @staticmethod
    def fill_none(data: Any, fill_value, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def values_as_type(data: Any, dtype, **kwargs):
        raise NotImplementedError()


class Uproot3Methods(ArrayMethodsProtocol):
    """
    Provides uproot4-specific methods for the dict-like interface.
    """

    @staticmethod
    def all(data: Any, **kwargs) -> bool:
        axis = kwargs.pop("axis", 1)
        if axis is None:
            return all(data.all(**kwargs))
        return data.all(**kwargs)

    @staticmethod
    def array_from_tree(tree: Any, key: str) -> Any:
        return tree.array(key)

    @staticmethod
    def awkward_from_iter(data: Iterable) -> Any:
        return ak0.fromiter(data)

    @staticmethod
    def contains(data: Any, key: str) -> bool:
        return key in data

    @staticmethod
    def evaluate(data: Any, expression: str, **kwargs) -> Any:
        ak1_array = ak.Array(data)
        ak1_result = Uproot4Methods.evaluate(ak1_array, expression, **kwargs)
        return ak0.fromiter(ak1_result)

    @staticmethod
    def num_entries(tree: Any) -> int:
        return tree.numentries


class Uproot4Methods(ArrayMethodsProtocol):
    """
    Provides uproot4-specific methods for the dict-like interface.
    """

    @staticmethod
    def all(data: Any, **kwargs) -> bool:
        axis = kwargs.pop("axis", 1)
        return ak.all(data, axis=axis, **kwargs)

    @staticmethod
    def array_from_tree(tree: Any, key: str) -> Any:
        return tree.__getitem__(key).array()

    @staticmethod
    def awkward_from_iter(data: Iterable) -> Any:
        return ak.from_iter(data)

    @staticmethod
    def evaluate(data: Any, expression: str, **kwargs) -> Any:
        return ak.numexpr.evaluate(expression, data, **kwargs)

    @staticmethod
    def contains(data: Any, key: str) -> bool:
        return key in data.keys()

    @staticmethod
    def num_entries(tree: Any) -> int:
        return tree.num_entries
