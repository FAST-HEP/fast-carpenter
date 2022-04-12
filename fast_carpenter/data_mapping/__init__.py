from typing import Callable, Dict

from .array_methods import Uproot4Methods
from .connectors import (
    ArrayLike,
    DataConnectorProtocol,
    DataMapping,
    FileConnector,
    FileLike,
    TreeConnector,
    TreeLike,
)
from .indexing import IndexProtocol, IndexWithAliases, MultiTreeIndex, TokenMapIndex

DATA_CONNECTORS: Dict[str, DataConnectorProtocol] = {}


def register_data_connector(name: str, connector_creation_func: Callable) -> None:
    """
    Register a data connector.
    """
    if name in DATA_CONNECTORS:
        raise ValueError(f"Data connector {name} already registered.")
    DATA_CONNECTORS[name] = connector_creation_func


def unregister_data_connector(name: str) -> None:
    """
    Unregister a connector.
    """
    DATA_CONNECTORS[name].pop()


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
    "Uproot4Methods",
    "TreeConnector",
    "IndexWithAliases",
    "TokenMapIndex",
    "MultiTreeIndex",
    "FileConnector",
]
