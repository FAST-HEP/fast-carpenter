from typing import Any, Dict, List, Protocol


class ArrayMethodsProtocol(Protocol):

    @staticmethod
    def arraydict_to_pandas(arraydict: Dict[str, Any]):
        """
        Converts a dictionary of arrays to a pandas DataFrame.
        """
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
    def all(data: Any, **kwargs) -> bool:
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
    def array_from_tree(self, tree: Any, key: str) -> Any:
        return tree.array(key)

    @staticmethod
    def contains(data: Any, key: str) -> bool:
        return key in data

    @staticmethod
    def num_entries(tree: Any) -> int:
        return tree.numentries


class Uproot4Methods(ArrayMethodsProtocol):
    """
    Provides uproot4-specific methods for the dict-like interface.
    """

    @staticmethod
    def array_from_tree(self, tree: Any, key: str) -> Any:
        return tree.__getitem__(key).array()

    @staticmethod
    def contains(data: Any, key: str) -> bool:
        return key in data.keys()

    @staticmethod
    def num_entries(tree: Any) -> int:
        return tree.num_entries
