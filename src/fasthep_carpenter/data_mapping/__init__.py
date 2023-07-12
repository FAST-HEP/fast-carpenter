from collections import abc
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Dict, List, Protocol

from ..protocols import ArrayMethodsProtocol, DataConnector, DataMapping, FileLike
from ..utils import register_in_collection, unregister_from_collection

from .array_methods import Uproot4Methods
from .connectors import (
    FileConnector,
    TreeConnector,
)
from .indexing import IndexWithAliases, MultiTreeIndex, TokenMapIndex

DATA_CONNECTORS: Dict[str, DataConnector] = {}
ARRAY_METHODS: Dict[str, ArrayMethodsProtocol] = {}

__all__ = [
    "DATA_CONNECTORS",
    "register_data_connector",
    "unregister_data_connector",
    "DataConnector",
    "DataMapping",
    "Uproot4Methods",
    "TreeConnector",
    "IndexWithAliases",
    "TokenMapIndex",
    "MultiTreeIndex",
    "FileConnector",
]

register_data_connector = partial(register_in_collection, DATA_CONNECTORS, "Data connectors")
unregister_data_connector = partial(unregister_from_collection, DATA_CONNECTORS, "Data connectors")

register_array_methods = partial(register_in_collection, ARRAY_METHODS, "Array methods")
unregister_array_methods = partial(unregister_from_collection, ARRAY_METHODS, "Array methods")

register_data_connector("tree", TreeConnector)
register_data_connector("file", FileConnector)
register_array_methods("uproot4", Uproot4Methods)


def __create_mapping_with_tree_connector__(
    input_file: FileLike, treenames: str, methods: str
) -> DataMapping:
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
) -> DataMapping:
    return DataMapping(
        connector=DATA_CONNECTORS["file"](
            file_handle=input_file, methods=ARRAY_METHODS[methods], treenames=treenames
        ),
        methods=ARRAY_METHODS[methods],
        indices=[IndexWithAliases({}), TokenMapIndex(), MultiTreeIndex(treenames)],
    )


def create_mapping(input_file: FileLike, treenames: str, methods: str, connector: str) -> DataMapping:
    if connector == "tree":
        return __create_mapping_with_tree_connector__(input_file, treenames, methods)
    if connector == "file":
        return __create_mapping_with_file_connector__(input_file, treenames, methods)

    raise ValueError(f"Connector {connector} not supported by this function.")


def create_ranged_mapping():
    pass


def create_masked_mapping():
    pass
