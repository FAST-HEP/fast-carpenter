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

    def __contains__(self, key: str) -> bool:
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


class TreeConnector(DataConnectorProtocol):
    _tree: TreeLike
    _methods: ArrayMethodsProtocol

    def __init__(self, **kwargs):
        self._tree = kwargs.pop("tree")
        self._methods = kwargs.pop("methods")

    def __contains__(self, key: str) -> bool:
        return self._methods.contains(self._tree, key)

    def get(self, key):
        return self._methods.array_from_tree(self._tree, key)

    @property
    def num_entries(self):
        return self._methods.num_entries(self._tree)

    def arrays(self, keys, **kwargs):
        pass

    def keys(self):
        return list(self._tree.keys())


class FileConnector(DataConnectorProtocol):
    _file_handle: FileLike
    _methods: ArrayMethodsProtocol
    _treenames: List[str]

    def __init__(self, **kwargs):
        self._file_handle = kwargs.pop("file_handle")
        self._treenames = kwargs.pop("treenames")
        self._methods = kwargs.pop("methods")

    def __contains__(self, key: str) -> bool:
        if len(self._treenames) == 1:
            if key in self._file_handle[self._treenames[0]]:
                return True
        try:
            value = self._file_handle[key]
            if value is not None:
                return True
        except Exception:
            pass
        return key in self._file_handle.keys()

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
        all_keys = chain(self.tree.keys(), self.aliases.keys(), self.extra_variables.keys())
        for key in all_keys:
            yield key
