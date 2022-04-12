from typing import Any, Dict, List, Protocol


class ArrayMethodsProtocol(Protocol):
    @staticmethod
    def num_entries(array) -> int:
        raise NotImplementedError()

    @staticmethod
    def arraydict_to_pandas(arraydict: Dict[str, Any]):
        """
        Converts a dictionary of arrays to a pandas DataFrame.
        """
        raise NotImplementedError()

    @staticmethod
    def array_dict(data: Any, keys: List[str]) -> Dict[str, Any]:
        """
        Returns a dictionary of arrays for the given keys.
        """
        raise NotImplementedError()

    @staticmethod
    def array_exporter(dict_of_arrays: Any, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def arrays(data: Any, expressions: str, *args, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def array(data: Any, key: str):
        raise NotImplementedError()

    @staticmethod
    def evaluate(data, expression, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def counts(data: Any, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def all(data: Any, **kwargs) -> bool:
        raise NotImplementedError()

    @staticmethod
    def pad(data, length: int, **kwargs: Dict[str, Any]) -> Any:
        raise NotImplementedError()

    @staticmethod
    def flatten(data, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def sum(data, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def prod(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def any(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def count_nonzero(array, **kwargs):
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
