from collections.abc import Iterable
from typing import Any, Dict, List

import awkward as ak
import pandas as pd

from ..protocols import ArrayMethodsProtocol


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
    def arraydict_to_pandas(arraydict: Dict[str, Any]) -> pd.DataFrame:
        return ak.to_pandas(arraydict)

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

    @staticmethod
    def arrays(data: Any, expressions: str, *args, **kwargs) -> Any:
        if "outputtype" in kwargs:
            # renamed uproot3 -> uproot4
            outputtype = kwargs.pop("outputtype")
            kwargs["how"] = outputtype
        operations = kwargs.get("operations", [])
        array_dict = Uproot4Methods.extract_array_dict(data, expressions)
        for operation in operations:
            for key, value in array_dict.items():
                array_dict[key] = operation(value)
        return Uproot4Methods.array_exporter(array_dict, **kwargs)

    @staticmethod
    def extract_array_dict(data: Any, keys: List[str]) -> Dict[str, Any]:
        """
        Returns a dictionary of arrays for the given keys.
        """
        extra_arrays = None
        _keys = keys.copy()
        if hasattr(data, "_extra_variables") and data._extra_variables:
            # check if any of the extra variables are included in the expressions
            extra_vars = []
            for name in data._extra_variables:
                if name in keys:
                    extra_vars.append(name)
                    _keys.remove(name)
            if extra_vars:
                extra_arrays = {key: data._extra_variables[key] for key in extra_vars}

        data_arrays = {}
        for key in _keys:
            data_arrays[key] = data[key]
        if extra_arrays is not None:
            data_arrays.update(extra_arrays)
        return data_arrays

    @staticmethod
    def array_exporter(dict_of_arrays: Any, **kwargs) -> Any:
        LIBRARIES = {
            "awkward": ["ak", "ak.Array", "awkward"],
            "numpy": ["np", "np.ndarray", "numpy"],
            "pandas": ["pd", "pd.DataFrame", "pandas"],
        }
        library = kwargs.get("library", "ak")
        how = kwargs.get("how", dict)
        if library in LIBRARIES["awkward"]:
            if how == dict:
                return dict_of_arrays
            elif how == list:
                return [value for value in dict_of_arrays.values()]
            elif how == tuple:
                return tuple(value for value in dict_of_arrays.values())

        if library in LIBRARIES["pandas"]:
            return Uproot4Methods.arraydict_to_pandas(dict_of_arrays)
