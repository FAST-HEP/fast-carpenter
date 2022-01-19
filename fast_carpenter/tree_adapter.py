# input is an uproot tree (uproot3 --> adapter V0, uproot4 --> adapter V1)
# output is a dict with the same structure as the tree
from collections import abc
from typing import Any, Callable, Dict, Optional

import awkward as ak
import numpy as np

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
    extra_variables: Dict[str, Any]

    def __init__(self, tree: Any, aliases: Dict[str, Any] = None) -> None:
        self.tree = tree
        self.aliases = aliases if aliases else {}
        self.extra_variables = {}

    def __getitem__(self, key: str) -> Any:
        """
        Get an item from the tree.
        Resolves aliases if defined.
        """
        return self.__m_getitem__(self.__resolve_key__(key))

    def __setitem__(self, key, value) -> None:
        """
        Creates a new branch in the tree.
        Resolves aliases if defined.
        """
        self.__m_setitem__(self.__resolve_key__(key), value)

    def __iter__(self) -> Any:
        return iter(self.tree)

    def __len__(self) -> int:
        """ Returns the number of branches in the tree. """
        return len(self.tree)

    def __delitem__(self, key) -> None:
        """ Deletes a branch from the tree. """
        self.__m_delitem__(self.__resolve_key__(key))

    def __contains__(self, key):
        return self.__m_contains__(self.__resolve_key__(key))

    def new_variable(self, key, value, context: Optional[Any] = None) -> None:
        if context is None:
            context = self
        if len(value) != context.num_entries:
            msg = f"New variable {key} does not have the right length: {len(value)} not {context.num_entries}"
            raise ValueError(msg)

        if key not in self:
            self.__new_variable__(self.__resolve_key__(key), value)
        else:
            raise ValueError(f"Trying to overwrite existing variable: {key}")

    def evaluate(self, expression: str) -> Any:
        """
        Evaluate a string expression using the tree as the namespace.
        """
        raise NotImplementedError()

    @property
    def num_entries(self) -> int:
        """ Returns the number of entries in the tree. """
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
    special_token_mapping: Dict[str, str] = {
        ".": "__DOT__",
    }

    def __resolve_key__(self, key):
        if key in self.aliases:
            return self.aliases[key]
        return key

    def __resolve_special_tokens__(self, key):
        new_key = key
        for token in self.special_token_mapping:
            if token in key:
                new_key = new_key.replace(token, self.special_token_mapping[token])
        return new_key


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

    @staticmethod
    def counts(array, **kwargs):
        return array.counts

    @staticmethod
    def pad(array, length, **kwargs):
        return array.pad(length, **kwargs)

    @staticmethod
    def flatten(array, **kwargs):
        return array.flatten(**kwargs)

    @staticmethod
    def sum(array, **kwargs):
        return array.sum(**kwargs)

    @staticmethod
    def prod(array, **kwargs):
        return array.prod(**kwargs)

    @staticmethod
    def any(array, **kwargs):
        return array.any(**kwargs)

    @staticmethod
    def all(array, **kwargs):
        return array.all(**kwargs)

    @staticmethod
    def count_nonzero(array, **kwargs):
        return array.count_nonzero(**kwargs)

    @staticmethod
    def max(array, **kwargs):
        return array.max(**kwargs)

    @staticmethod
    def min(array, **kwargs):
        return array.min(**kwargs)

    @staticmethod
    def argmax(array, **kwargs):
        return array.argmax(**kwargs)

    @staticmethod
    def argmin(array, **kwargs):
        return array.argmin(**kwargs)

    @staticmethod
    def count_zero(array, **kwargs):
        return np.count_zero(array, **kwargs)


class Uproot4Methods(object):
    """
    Provides uproot4-specific methods for the dict-like interface.
    """

    def __m_getitem__(self, key):
        if key in self.extra_variables:
            return self.extra_variables[key]
        return self.tree[key].array()

    def __m_setitem__(self, key, value):
        self.tree.set_branch(key, value)

    def __m_delitem__(self, key):
        self.tree.drop_branch(key)

    def __m_contains__(self, key):
        return key in self.tree.keys() or key in self.extra_variables.keys()

    def __new_variable__(self, key, value) -> None:
        key = self.__resolve_special_tokens__(key)
        self.extra_variables[key] = value

    @property
    def num_entries(self) -> int:
        return self.tree.num_entries

    def arrays(self, *args, **kwargs):
        if "outputtype" in kwargs:
            # renamed uproot3 -> uproot4
            outputtype = kwargs.pop("outputtype")
            kwargs["how"] = outputtype
        return self.tree.arrays(*args, **kwargs)

    def array(self, key):
        return self[key]

    def evaluate(self, expression, **kwargs):
        return ak.numexpr.evaluate(expression, self, **kwargs)

    @staticmethod
    def counts(array, **kwargs):
        axis = kwargs.pop("axis", 1)
        return ak.count(array, axis=axis, **kwargs)

    @staticmethod
    def all(array, **kwargs):
        axis = kwargs.pop("axis", 1)
        return ak.all(array, axis=axis, **kwargs)

    @staticmethod
    def pad(array, length: int, **kwargs: Dict[str, Any]) -> ak.Array:
        axis = kwargs.pop("axis", -1)
        return ak.pad_none(array, length, axis=axis, **kwargs)

    @staticmethod
    def flatten(array, **kwargs):
        return ak.flatten(array, **kwargs)

    @staticmethod
    def sum(array, **kwargs):
        axis = kwargs.pop("axis", -1)
        return ak.sum(array, axis=axis, **kwargs)

    @staticmethod
    def prod(array, **kwargs):
        return ak.prod(array, **kwargs)

    @staticmethod
    def any(array, **kwargs):
        axis = kwargs.pop("axis", -1)
        return ak.any(array, axis=axis, **kwargs)

    @staticmethod
    def count_nonzero(array, **kwargs):
        return ak.count_nonzero(array, **kwargs)

    @staticmethod
    def max(array, **kwargs):
        axis = kwargs.pop("axis", 1)
        return ak.max(array, axis=axis, **kwargs)

    @staticmethod
    def min(array, **kwargs):
        return ak.min(array, **kwargs)

    @staticmethod
    def argmax(array, **kwargs):
        return ak.argmax(array, **kwargs)

    @staticmethod
    def argmin(array, **kwargs):
        return ak.argmin(array, **kwargs)

    @staticmethod
    def count_zero(array, **kwargs):
        return ak.count_zero(array, **kwargs)


ArrayMethods = Uproot4Methods


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
        """Returns the size of the range - overwrites tree.num_entries."""
        return self.block_size

    def __getitem__(self, key):
        return self.tree[key][self.start:self.stop]

    def __setitem__(self, key, value):
        self.tree[key][self.start:self.stop] = value[self.start:self.stop]

    def __delitem__(self, key):
        del self.tree[key][self.start:self.stop]

    def __contains__(self, key):
        return key in self.tree

    def __len__(self):
        return self.block_size

    def array(self, key):
        return self[key]

    def arrays(self, *args, **kwargs):
        arrays = self.tree.arrays(*args, **kwargs)
        return [array[self.start:self.stop] for array in arrays]

    def new_variable(self, name, value):
        import awkward as ak
        if self.block_size < self.tree.num_entries:
            new_value = ak.concatenate(
                [
                    ak.Array([None] * self.start),
                    value,
                    ak.Array([None] * (self.tree.num_entries - self.stop))
                ],
                axis=0
            )
        self.tree.new_variable(name, new_value, context=self.tree)

    def evaluate(self, expression, **kwargs):
        import awkward as ak
        return ak.numexpr.evaluate(expression, self, **kwargs)


def create_ranged(arguments: Dict[str, Any]) -> Ranger:
    """
    Create a tree adapter with ranged access.
    """
    args_copy = arguments.copy()

    start = args_copy.pop("start")
    stop = args_copy.pop("stop")
    tree = create(args_copy)

    return Ranger(tree, start, stop)
