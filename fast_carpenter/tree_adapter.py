# input is an uproot tree (uproot3 --> adapter V0, uproot4 --> adapter V1)
# output is a dict with the same structure as the tree
from collections.abc import MutableMapping
from typing import Any, Dict


class TreeToDictAdaptorV0(MutableMapping):

    def __init__(self, tree: Any, aliases: Dict[str, Any] = None) -> None:
        self.tree = tree
        self.aliases = aliases
        # self.tree_dict = {}
        # self.tree_dict_keys = []

    def __getitem__(self, key: str) -> Any:
        return self.tree[key]

    def __setitem__(self, key, value) -> None:
        self.tree[key] = value

    def __iter__(self) -> Any:
        return iter(self.tree)

    def __len__(self) -> int:
        return len(self.tree)

    def __delitem__(self, key) -> None:
        del self.tree[key]

    @property
    def num_entries(self) -> int:
        return self.tree.numentries


class TreeToDictAdaptorV1(MutableMapping):

    def __init__(self, tree: Any, aliases: Dict[str, Any] = None) -> None:
        self.tree = tree
        self.aliases = aliases
        # self.tree_dict = {}
        # self.tree_dict_keys = []

    def __getitem__(self, key: str) -> Any:
        return self.tree[key]

    def __setitem__(self, key, value) -> None:
        self.tree[key] = value

    def __iter__(self) -> Any:
        return iter(self.tree)

    def __len__(self) -> int:
        return len(self.tree)

    def __delitem__(self, key) -> None:
        del self.tree[key]

    @property
    def num_entries(self) -> int:
        return self.tree.num_entries
