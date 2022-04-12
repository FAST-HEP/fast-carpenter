from collections import abc
from dataclasses import field
from typing import Dict, List, Protocol

from .array_methods import ArrayMethodsProtocol
from .indexing import IndexProtocol


class FileLike(Protocol):
    pass


class ArrayLike(Protocol):
    pass


class TreeLike(Protocol):
    pass


class DataConnectorProtocol(Protocol):
    """Read-only data connector"""

    def __init__(self, **kwargs):
        pass

    def get(self, key):
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

    def __init__(
        self,
        connector: DataConnectorProtocol,
        methods: ArrayMethodsProtocol,
        indices=None,
    ):
        self._connector = connector
        self._methods = methods
        self._extra_variables = {}
        if indices is not None:
            self._indices = indices

    def __getitem__(self, key):
        return None

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        return len(self._connector)

    @property
    def num_entries(self):
        return self._connector.num_entries


class TreeConnector(DataConnectorProtocol):
    _tree: TreeLike
    _methods: ArrayMethodsProtocol

    def __init__(self, **kwargs):
        self._tree = kwargs.pop("tree")
        self._methods = kwargs.pop("methods")

    def get(self, key):
        return self._methods.array_from_tree(self._tree, key)

    @property
    def num_entries(self):
        return self._methods.num_entries(self._tree)

    def arrays(self, keys, **kwargs):
        pass

    def keys(self):
        pass


class FileConnector(DataConnectorProtocol):
    _file_handle: FileLike
    _methods: ArrayMethodsProtocol
    _treenames: List[str]

    def __init__(self, **kwargs):
        self._file_handle = kwargs.pop("file_handle")
        self._treenames = kwargs.pop("treenames")
        self._methods = kwargs.pop("methods")

    def get(self, key):
        return self._methods.array_from_file(self._file, key)

    @property
    def num_entries(self):
        """Returns the number of entries in the file."""
        lengths = [
            self._methods.num_entries(self._file_handle[treename])
            for treename in self._treenames
        ]
        return max(lengths)

    def arrays(self, keys, **kwargs):
        pass

    def keys(self):
        pass
