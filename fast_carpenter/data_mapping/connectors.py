from collections import abc
from dataclasses import field
from typing import Dict, List, Protocol

from .indexing import IndexProtocol
from .array_methods import ArrayMethodsProtocol


class FileLike(Protocol):
    pass


class ArrayLike(Protocol):
    pass


class TreeLike(Protocol):
    pass


class DataConnectorProtocol(Protocol):
    def __init__(self, data):
        pass

    def __getattr__(self, name):
        pass

    def __getitem__(self, key):
        pass

    @property
    def num_entries(self):
        pass

    def arrays(self, keys, **kwargs):
        pass

    def keys(self):
        pass


class DataMapping(abc.MutableMapping):
    _connector: DataConnectorProtocol
    _methods: ArrayMethodsProtocol
    _extra_variables: Dict[str, ArrayLike]
    _indices: List[IndexProtocol] = field(default_factory=list)

    def __init__(self, data, connector, methods, indices=None):
        self._connector = connector(data)
        self._methods = methods
        self._extra_variables = {}
        if indices is not None:
            self._indices = indices
        pass
