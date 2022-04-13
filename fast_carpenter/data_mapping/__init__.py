from collections import abc
from dataclasses import field
from functools import partial
from typing import Any, Dict, List, Protocol

from .array_methods import ArrayMethodsProtocol, Uproot3Methods, Uproot4Methods
from .connectors import (
    ArrayLike,
    DataConnectorProtocol,
    FileConnector,
    FileLike,
    TreeConnector,
    TreeLike,
)
from .indexing import IndexProtocol, IndexWithAliases, MultiTreeIndex, TokenMapIndex

DATA_CONNECTORS: Dict[str, DataConnectorProtocol] = {}
ARRAY_METHODS: Dict[str, ArrayMethodsProtocol] = {}

__all__ = [
    "DATA_CONNECTORS",
    "register_data_connector",
    "unregister_data_connector",
    "DataConnectorProtocol",
    "DataMapping",
    "IndexProtocol",
    "ArrayLike",
    "TreeLike",
    "FileLike",
    "Uproot3Methods",
    "Uproot4Methods",
    "TreeConnector",
    "IndexWithAliases",
    "TokenMapIndex",
    "MultiTreeIndex",
    "FileConnector",
]


def __register__(
    collection: Dict[str, Any], collection_name: str, name: str, obj: Any
) -> None:
    if name in collection:
        raise ValueError(f"{collection_name} {name} already registered.")
    collection[name] = obj


def __unregister__(
    collection: Dict[str, Any], collection_name: str, name: str, obj: Any
) -> None:
    if name not in collection:
        raise ValueError(f"{collection_name} {name} not registered.")
    collection[name].pop()


register_data_connector = partial(__register__, DATA_CONNECTORS, "Data collector")
unregister_data_connector = partial(__unregister__, DATA_CONNECTORS, "Data collector")

register_array_methods = partial(__register__, ARRAY_METHODS, "Array methods")
unregister_array_methods = partial(__unregister__, ARRAY_METHODS, "Array methods")

register_data_connector("tree", TreeConnector)
register_data_connector("file", FileConnector)
register_array_methods("uproot3", Uproot3Methods)
register_array_methods("uproot4", Uproot4Methods)


class DataWrapperProtocol(Protocol):
    pass


class DoNothingDataWrapper(DataWrapperProtocol):
    def __init__(self, **kwargs):
        pass

    def __call__(self, data):
        return data


class RangedDataWrapper(DataWrapperProtocol):
    def __init__(self, **kwargs):
        self._start = kwargs.get("start", 0)
        self._stop = kwargs.get("stop", None)

    def __call__(self, data):
        return data


class MaskedDataWrapper(DataWrapperProtocol):
    def __init__(self, **kwargs):
        self._mask = kwargs.get("mask", None)

    def __call__(self, data):
        return data


class DataMapping(abc.MutableMapping):
    _connector: DataConnectorProtocol
    _methods: ArrayMethodsProtocol
    _data_wrappers: List[DataWrapperProtocol]
    _extra_variables: Dict[str, ArrayLike]
    _indices: List[IndexProtocol] = field(default_factory=list)

    def __init__(
        self,
        connector: DataConnectorProtocol,
        methods: ArrayMethodsProtocol,
        data_wrappers: List[DataWrapperProtocol] = None,
        indices=None,
    ):
        self._connector = connector
        self._methods = methods
        if data_wrappers is None:
            self._data_wrappers = [DoNothingDataWrapper()]
        else:
            self._data_wrappers = data_wrappers

        self._extra_variables = {}
        if indices is not None:
            self._indices = indices

    def __contains__(self, name):
        in_extra = name in self._extra_variables
        in_connector = name in self._connector
        return in_extra or in_connector

    def __getitem__(self, key):
        if key in self._extra_variables:
            return self._extra_variables[key]
        return None

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        return len(self._connector)

    def add_variable(self, name, value):
        if name not in self:
            self._extra_variables[name] = value
        else:
            raise ValueError(f"Trying to overwrite existing variable: {name}")

    @property
    def num_entries(self):
        return self._connector.num_entries


def __create_mapping_with_tree_connector__(
    input_file: FileLike, treenames: str, methods: str
):
    if len(treenames) > 1:
        raise NotImplementedError(
            "Multiple trees not supported by the TreeConnector. Please use a different connector."
        )
    return DataMapping(
        connector=DATA_CONNECTORS["tree"](
            tree=input_file[treenames[0]], methods=ARRAY_METHODS[methods]
        ),
        methods=ARRAY_METHODS[methods],
        indices=[IndexWithAliases({}), TokenMapIndex()],
    )


def __create_mapping_with_file_connector__(
    input_file: FileLike, treenames: str, methods: str
):
    return DataMapping(
        connector=DATA_CONNECTORS["file"](
            file_handle=input_file, methods=ARRAY_METHODS[methods], treenames=treenames
        ),
        methods=ARRAY_METHODS[methods],
        indices=[IndexWithAliases({}), TokenMapIndex(), MultiTreeIndex(treenames)],
    )


def create_mapping(input_file: FileLike, treenames: str, methods: str, connector: str):
    if connector == "tree":
        return __create_mapping_with_tree_connector__(input_file, treenames, methods)
    if connector == "file":
        return __create_mapping_with_file_connector__(input_file, treenames, methods)

    raise ValueError(f"Connector {connector} not supported by this function.")


def create_ranged_mapping():
    pass


def create_masked_mapping():
    pass
