# input is an uproot tree (uproot3 --> adapter V0, uproot4 --> adapter V1)
# output is a dict with the same structure as the tree
from collections import abc
import logging
from typing import Any, Callable, Dict, Optional

import awkward as ak
import numpy as np

adapters: Dict[str, Callable] = {}
DEFAULT_TREE_TO_DICT_ADAPTOR = "uproot4"
logger = logging.getLogger(__name__)


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
        return array.compact().counts

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

    @staticmethod
    def dtype(array, **kwargs):
        return array.dtype.kind

    @staticmethod
    def is_bool(array, **kwargs):
        return Uproot3Methods.dtype(array) == "b"

    @staticmethod
    def arrays_as_np_lists(data, array_names, **kwargs):
        return data.arrays(array_names, outputtype=lambda *args: np.array(args))


class Uproot4Methods(object):
    """
    Provides uproot4-specific methods for the dict-like interface.
    """

    def __m_getitem__(self, key):
        if key in self.extra_variables:
            return self.extra_variables[key]
        if hasattr(self.tree[key], "array"):
            return self.tree[key].array()
        return self.tree[key]

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
        try:
            return self.tree.num_entries
        except AttributeError as e:
            logger.error(f"Object of type {type(self.tree)} does not have a num_entries attribute.")
            raise e

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
        axis = kwargs.pop("axis", None)
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

    @staticmethod
    def dtype(array, **kwargs):
        t = ak.type(array).type
        if hasattr(t, "dtype"):
            return t.dtype
        return t

    @staticmethod
    def is_bool(array, **kwargs):
        t = Uproot4Methods.dtype(array)
        if "bool" in str(t):
            return True
        return Uproot4Methods.dtype(array) == "bool"

    @staticmethod
    def arrays_as_np_lists(data, array_names, **kwargs):
        return np.array(data.arrays(array_names, library="np", outputtype=list))


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
    mask: Any

    def __init__(self, tree: TreeToDictAdaptor, start: int, stop: int) -> None:
        self.tree = tree
        self.start = start
        self.stop = stop
        self.block_size = stop - start if stop > start > 0 else tree.num_entries
        self.mask = None

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


def combine_masks(masks):
    import awkward as ak
    if len(masks) == 0:
        return ak.Array([])
    elif len(masks) == 1:
        return masks[0]
    else:
        return ak.concatenate(masks, axis=0)


class Masked(object):
    _mask: Any
    _tree: Ranger

    def __init__(self, tree: Ranger, mask: Any) -> None:
        self._tree = tree
        self._mask = mask

    def __getitem__(self, key):
        if self._mask is None:
            return self._tree[key]
        try:
            if len(self._mask) > len(self._tree):
                return self._tree[key][self._tree.start:self._tree.stop].mask[self._mask]
        except:
            raise TypeError()
        return self._tree[key].mask[self._mask]

    def __len__(self):
        return len(self._tree)

    @property
    def num_entries(self) -> int:
        return self._tree.num_entries

    def count_nonzero(self):
        if self._mask is None:
            return len(self._tree)
        return ak.count_nonzero(self._mask)

    def apply_mask(self, mask):
        if self._mask is None:
            self._mask = mask
        else:
            self._mask = self._mask & mask

    def array(self, key):
        return self[key]

    def arrays(self, *args, **kwargs):
        arrays = self._tree.arrays(*args, **kwargs)
        if self._mask is None:
            return arrays
        return [array[self._mask] for array in arrays]

    def evaluate(self, expression, **kwargs):
        import awkward as ak
        return ak.numexpr.evaluate(expression, self, **kwargs)


def create(arguments: Dict[str, Any]) -> TreeToDictAdaptor:
    """
    Create a TreeToDictAdaptor from a tree.
    """
    args_copy = arguments.copy()
    adapter_type = args_copy.pop("adapter", DEFAULT_TREE_TO_DICT_ADAPTOR)

    try:
        creation_func = adapters[adapter_type]
        return creation_func(**args_copy)
    except KeyError:
        raise ValueError(f"No adapter named {adapter_type}")


def create_ranged(arguments: Dict[str, Any]) -> Ranger:
    """
    Create a tree adapter with ranged access.
    """
    args_copy = arguments.copy()

    start = args_copy.pop("start", 0)
    stop = args_copy.pop("stop", -1)
    tree = create(args_copy)

    return Ranger(tree, start, stop)


def create_masked(arguments: Dict[str, Any]) -> Masked:
    """
    Create a tree adapter with masked access.
    """
    args_copy = arguments.copy()

    mask = args_copy.pop("mask", None)
    tree = create_ranged(args_copy)

    return Masked(tree, mask)


def create_masked_multitree(arguments: Dict[str, Any]) -> Masked:
    raise NotImplementedError("Multitree not yet implemented")
