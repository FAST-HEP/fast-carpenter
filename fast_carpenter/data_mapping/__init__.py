from collections import abc
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Dict, List, Protocol

from .array_methods import ArrayMethodsProtocol, Uproot3Methods, Uproot4Methods
from .connectors import (
    ArrayLike,
    DataConnector,
    FileConnector,
    FileLike,
    TreeConnector,
    TreeLike,
)
from .indexing import IndexProtocol, IndexWithAliases, MultiTreeIndex, TokenMapIndex

DATA_CONNECTORS: Dict[str, DataConnector] = {}
ARRAY_METHODS: Dict[str, ArrayMethodsProtocol] = {}

__all__ = [
    "DATA_CONNECTORS",
    "register_data_connector",
    "unregister_data_connector",
    "DataConnector",
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


register_data_connector = partial(__register__, DATA_CONNECTORS, "Data connectors")
unregister_data_connector = partial(__unregister__, DATA_CONNECTORS, "Data connectors")

register_array_methods = partial(__register__, ARRAY_METHODS, "Array methods")
unregister_array_methods = partial(__unregister__, ARRAY_METHODS, "Array methods")

register_data_connector("tree", TreeConnector)
register_data_connector("file", FileConnector)
register_array_methods("uproot3", Uproot3Methods)
register_array_methods("uproot4", Uproot4Methods)


@dataclass
class DatasetConfig:
    # TODO: to be simplified/removed later
    name: str
    eventtype: str


@dataclass
class ConfigProxy:
    # TODO: to be simplified/removed later
    dataset: DatasetConfig


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
    _connector: DataConnector
    _methods: ArrayMethodsProtocol
    _data_wrappers: List[DataWrapperProtocol]
    _extra_variables: Dict[str, ArrayLike]
    _indices: List[IndexProtocol] = field(default_factory=list)
    _config: ConfigProxy

    def __init__(
        self,
        connector: DataConnector,
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
        self._config = None

    def __resolve_key__(self, key):
        if key in self._extra_variables:
            return key
        if self._indices:
            lookup = key
            for index in reversed(self._indices):
                lookup = index.resolve_index(lookup)
                if lookup in self._connector:
                    return lookup
        return key

    def __contains__(self, name):
        if name in self._extra_variables:
            return True
        if self._indices:
            lookup = name
            for index in reversed(self._indices):
                lookup = index.resolve_index(lookup)
                if lookup in self._connector:
                    return True

        return name in self._connector

    def __getitem__(self, key):
        if isinstance(key, abc.Sequence) and not isinstance(key, str):
            return self.__getitems__(key)
        if key in self._extra_variables:
            return self._extra_variables[key]
        lookup = key
        for index in reversed(self._indices):
            lookup = index.resolve_index(lookup)
            if lookup in self._connector:
                return self._connector[lookup]
        raise KeyError(f"{key} not found in {self._connector}")

    def __getitems__(self, keys):
        all_string = all(isinstance(key, str) for key in keys)
        if all_string:
            return self.arrays(keys)

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

    def new_variable(self, name, value):
        """Adapter for legacy method"""
        return self.add_variable(name, value)

    def evaluate(self, expression, **kwargs):
        return self._methods.evaluate(self, expression, **kwargs)

    @property
    def num_entries(self):
        return self._connector.num_entries

    @property
    def tree(self):
        return self

    def arrays(self, keys: abc.Sequence, **kwargs) -> List[ArrayLike]:
        if "outputtype" in kwargs:
            # renamed uproot3 -> uproot4
            outputtype = kwargs.pop("outputtype")
            kwargs["how"] = outputtype
        kwargs["how"] = kwargs.get("how", tuple)
        return self._methods.arrays(self, keys, **kwargs)

    def keys(self):
        all_keys = set(self._connector.keys())
        all_keys.update(self._extra_variables.keys())
        return all_keys

    def add_dataset_info(self, dataset_name: str, dataset_eventtype: str) -> None:
        self._config = ConfigProxy(DatasetConfig(dataset_name, dataset_eventtype))

    @property
    def config(self):
        return self._config

    def apply_mask(self, mask):
        self._data_wrappers = [MaskedDataWrapper(mask=mask)] + self._data_wrappers


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
