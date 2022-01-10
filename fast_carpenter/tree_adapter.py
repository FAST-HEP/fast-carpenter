# input is an uproot tree (uproot3 --> adapter V0, uproot4 --> adapter V1)
# output is a dict with the same structure as the tree
from collections import abc
from typing import Any, Callable, Dict

adapters: Dict[str, Callable] = {}


def register(name: str, adaptor_creation_func: Callable) -> None:
    """
    Register an adaptor.
    """
    adapters[name] = adaptor_creation_func


def unregister(name: str) -> None:
    """
    Unregister an adaptor.
    """
    adapters[name].pop()


class TreeToDictAdaptor(abc.MutableMapping):
    """
    Provides a dict-like interface to a tree-like data object (e.g. ROOT TTree, uproot.tree, etc).
    """

    tree: Any
    aliases: Dict[str, Any]

    def __init__(self, tree: Any, aliases: Dict[str, Any] = None) -> None:
        self.tree = tree
        self.aliases = aliases if aliases else {}

    def __getitem__(self, key: str) -> Any:
        return self.__m_getitem__(self.__resolve_key__(key))

    def __setitem__(self, key, value) -> None:
        self.__m_setitem__(self.__resolve_key__(key), value)

    def __iter__(self) -> Any:
        return iter(self.tree)

    def __len__(self) -> int:
        return len(self.tree)

    def __delitem__(self, key) -> None:
        self.__m_delitem__(self.__resolve_key__(key))

    def evaluate(self, expression: str) -> Any:
        """
        Evaluate a string expression using the tree as the namespace.
        """
        raise NotImplementedError()

    @property
    def num_entries(self) -> int:
        raise NotImplementedError()


def create(arguments: Dict[str, Any]) -> TreeToDictAdaptor:
    """
    Create a TreeToDictAdaptor from a tree.
    """
    args_copy = arguments.copy()
    adapter_type = args_copy.pop("adapter")

    try:
        creation_func = adapters[adapter_type]
        return creation_func(**args_copy)
    except KeyError:
        raise ValueError(f"No adapter named {adapter_type}")


class IndexingMixin(object):
    """
    Provides indexing support for the dict-like interface.
    """
    aliases: Dict[str, str]

    def __resolve_key__(self, key):
        if key in self.aliases:
            return self.aliases[key]
        return key


class Uproot3Methods(object):
    """
    Provides uproot3-specific methods for the dict-like interface.
    """

    def __m_getitem__(self, key):
        return self.tree.array(key)

    def __m_setitem__(self, key, value):
        self.tree.set_branch(key, value)

    def __m_delitem__(self, key):
        self.tree.drop_branch(key)

    def __m_contains__(self, key):
        return key in self.tree

    @property
    def num_entries(self) -> int:
        return self.tree.numentries


class Uproot4Methods(object):
    """
    Provides uproot4-specific methods for the dict-like interface.
    """

    def __m_getitem__(self, key):
        return self.tree[key].array()

    def __m_setitem__(self, key, value):
        self.tree.set_branch(key, value)

    def __m_delitem__(self, key):
        self.tree.drop_branch(key)

    def __m_contains__(self, key):
        return key in self.tree

    @property
    def num_entries(self) -> int:
        return self.tree.num_entries

    def arrays(self, *args, **kwargs):
        return self.tree.arrays(*args, **kwargs)

    def array(self, key):
        return self[key]

    def evaluate(self, expression):
        import awkward as ak
        return ak.numexpr.evaluate(expression, self)


class TreeToDictAdaptorV0(IndexingMixin, Uproot3Methods, TreeToDictAdaptor):
    pass


class TreeToDictAdaptorV1(IndexingMixin, Uproot4Methods, TreeToDictAdaptor):
    pass


register("uproot3", TreeToDictAdaptorV0)
register("uproot4", TreeToDictAdaptorV1)


class Ranger(object):
    tree: TreeToDictAdaptor
    start: int
    stop: int
    block_size: int

    def __init__(self, tree: TreeToDictAdaptor, start: int, stop: int) -> None:
        self.tree = tree
        self.start = start
        self.stop = stop
        self.block_size = stop - start

    @property
    def num_entries(self) -> int:
        return self.block_size

    def __getitem__(self, key):
        return self.tree[key][self.start:self.stop]

    def __setitem__(self, key, value):
        self.tree[key][self.start:self.stop] = value

    def __delitem__(self, key):
        del self.tree[key][self.start:self.stop]


def create_ranged(arguments: Dict[str, Any]) -> Ranger:
    """
    Create a ranger from a tree.
    """
    args_copy = arguments.copy()

    start = args_copy.pop("start")
    stop = args_copy.pop("stop")
    tree = create(args_copy)

    return Ranger(tree, start, stop)
