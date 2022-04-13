from itertools import chain
from typing import Any, List, Protocol

from .array_methods import ArrayMethodsProtocol


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

    def get(self, key: str) -> Any:
        pass

    @property
    def num_entries(self) -> int:
        pass

    def arrays(self, keys: List[str], **kwargs) -> Any:
        pass

    def keys(self) -> List[str]:
        pass


class TreeConnector(DataConnectorProtocol):
    _tree: TreeLike
    _methods: ArrayMethodsProtocol

    def __init__(self, **kwargs):
        self._tree = kwargs.pop("tree")
        self._methods = kwargs.pop("methods")

    def __contains__(self, key: str) -> bool:
        return self._methods.contains(self._tree, key)

    def get(self, key: str) -> Any:
        return self._methods.array_from_tree(self._tree, key)

    @property
    def num_entries(self) -> int:
        return self._methods.num_entries(self._tree)

    def arrays(self, keys: List[str], **kwargs) -> Any:
        pass

    def keys(self) -> List[str]:
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

    def get(self, key: str) -> Any:
        return self._methods.array_from_file(self._file, key)

    @property
    def num_entries(self) -> int:
        """Returns the number of entries of the largest tree in the file."""
        lengths = [
            self._methods.num_entries(self._file_handle[treename])
            for treename in self._treenames
        ]
        return max(lengths)

    def arrays(self, keys: List[str], **kwargs) -> Any:
        pass

    def keys(self) -> List[str]:
        tree_keys = [self._file_handle[treename].keys() for treename in self._treenames]
        all_keys = chain(self._file_handle.keys(), *tree_keys)
        yield from all_keys
