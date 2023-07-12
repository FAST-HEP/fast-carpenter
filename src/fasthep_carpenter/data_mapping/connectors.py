from collections import abc
from itertools import chain
from typing import Any, List

from ..protocols import ArrayMethodsProtocol, DataConnector, FileLike, TreeLike


class TreeConnector(DataConnector):
    """A connector for a tree-like object, to be used if the file contains only one tree."""
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

    def array_dict(self, keys: List[str], **kwargs) -> Any:
        pass

    def keys(self) -> List[str]:
        return list(self._tree.keys())

    def __delitem__(self, __v) -> None:
        return super().__delitem__(__v)

    def __setitem__(self, __k, __v) -> None:
        return super().__setitem__(__k, __v)

    def __getitem__(self, __k) -> Any:
        return self._tree[__k].array()

    def __iter__(self) -> Any:
        return super().__iter__()

    def __len__(self) -> int:
        return self.num_entries


class FileConnector(DataConnector):
    """A connector for a file-like object, to be used if the file contains multiple trees."""
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
        except KeyError:
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

    def array_dict(self, keys: List[str], **kwargs) -> Any:
        results = {}
        for key in keys:
            results[key] = self._file_handle[key].array()
        return results

    def keys(self) -> List[str]:
        tree_keys = [self._file_handle[treename].keys() for treename in self._treenames]
        all_keys = chain(self._file_handle.keys(), *tree_keys)
        yield from all_keys

    def __delitem__(self, __v) -> None:
        return super().__delitem__(__v)

    def __setitem__(self, __k, __v) -> None:
        return super().__setitem__(__k, __v)

    def __getitem__(self, __k) -> Any:
        return self._file_handle[__k].array()

    def __iter__(self) -> Any:
        return super().__iter__()

    def __len__(self) -> int:
        return self.num_entries
