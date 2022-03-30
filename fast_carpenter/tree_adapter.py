# input is an uproot tree (uproot3 --> adapter V0, uproot4 --> adapter V1)
# output is a dict with the same structure as the tree
from collections import abc
from dataclasses import field
from itertools import chain
import logging
from typing import Any, Callable, Dict, List, Optional, Protocol

import awkward as ak
import numpy as np

adapters: Dict[str, Callable] = {}
DEFAULT_TREE_TO_DICT_ADAPTOR = "uproot4"
logger = logging.getLogger(__name__)

LIBRARIES = {
    "awkward": ["ak", "ak.Array", "awkward"],
    "numpy": ["np", "np.ndarray", "numpy"],
    "pandas": ["pd", "pd.DataFrame", "pandas"],
}

SUPPORTED_OUTPUT_TYPES = [dict, tuple, list]


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


class FileLike(Protocol):
    pass


class ArrayLike(Protocol):
    pass


class TreeLike(Protocol):
    pass


class IndexProtocol(Protocol):
    def resolve_index(self, index: str) -> str:
        pass


class IndexWithAliases(IndexProtocol):
    aliases: Dict[str, str] = field(default_factory=dict)

    def __init__(self, aliases):
        self.aliases = aliases
        if self.aliases is None:
            self.aliases = {}

    def resolve_index(self, index):
        if not self.aliases:
            return index
        if index in self.aliases:
            return self.aliases[index]
        return self.index


class TokenMapIndex(IndexProtocol):
    token_map: Dict[str, str] = {
        ".": "__DOT__",
    }

    def __init__(self, token_map):
        self.token_map = token_map

    def resolve_index(self, index):
        new_index = index
        for token in self.token_map:
            if token in index:
                new_index = new_index.replace(token, self.token_map[token])
        return new_index


class MultiTreeIndex(IndexProtocol):
    prefix: str = ""

    def __init__(self, prefix: Optional[str] = None):
        self.prefix = prefix

    def resolve_index(self, index):
        index = index.replace(".", "/")
        if self.prefix:
            return f"{self.prefix}/{index}"
        return index


class TreeToDictAdaptor(abc.MutableMapping):
    """
    Provides a dict-like interface to a tree-like data object (e.g. ROOT TTree, uproot.tree, etc).
    """

    tree: TreeLike
    extra_variables: Dict[str, ArrayLike]
    indices: List[IndexProtocol] = field(default_factory=list)

    def __init__(self, tree: Any, aliases: Dict[str, str] = None) -> None:
        self.tree = tree
        self.extra_variables = {}
        self.indices = [
            IndexWithAliases(aliases),
            TokenMapIndex(token_map={".": "__DOT__", }),
        ]

    def __resolve_key__(self, key: str) -> str:
        """
        Resolve aliases and token map.
        """
        for index in self.indices:
            key = index.resolve_index(key)
        return key

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

    @property
    def aliases(self) -> Dict[str, str]:
        """
        Returns a dict of aliases.
        """
        result = {}
        for index in self.indices:
            if hasattr(index, "aliases"):
                result.update(index.aliases)
        return result

    def keys(self) -> List[str]:
        all_keys = chain(self.tree.keys(), self.aliases.keys(), self.extra_variables.keys())
        for key in all_keys:
            yield key

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
        if hasattr(array, "compact"):
            return array.compact().counts
        else:
            return array.counts

    def array(self, key):
        return self[key]

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

    @staticmethod
    def to_pandas(data, keys, flatten):
        return data.pandas.df(keys, flatten=flatten)


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
        key = self.__resolve_key__(key)
        self.extra_variables[key] = value

    @property
    def num_entries(self) -> int:
        try:
            return self.tree.num_entries
        except AttributeError as e:
            logger.error(f"Object of type {type(self.tree)} does not have a num_entries attribute.")
            raise e

    @staticmethod
    def arraydict_to_pandas(arraydict: Dict[str, Any]):
        """
        Converts a dictionary of arrays to a pandas DataFrame.
        """
        return ak.to_pandas(arraydict)

    def array_dict(self, keys: List[str]) -> Dict[str, Any]:
        """
        Returns a dictionary of arrays for the given keys.
        """
        extra_arrays = None
        _keys = keys.copy()
        if self.extra_variables:
            # check if any of the extra variables are included in the expressions
            extra_vars = []
            for name in self.extra_variables:
                if name in keys:
                    extra_vars.append(name)
                    _keys.remove(name)
            if extra_vars:
                extra_arrays = {key: self.extra_variables[key] for key in extra_vars}

        tree_arrays = self.tree.arrays(_keys, library="ak", how=dict)
        if extra_arrays is not None:
            tree_arrays.update(extra_arrays)
        return tree_arrays

    @staticmethod
    def array_exporter(dict_of_arrays, **kwargs):
        library = kwargs.get("library", "ak")
        how = kwargs.get("how", dict)

        # TODO: long-term we want exporters + factory methods for these
        # e.g. {("ak", tuple): AKArrayTupleExporter, ("numpy", tuple): NumpyArrayTupleExporter}
        # reason: we want to be able to use these exporters with different libraries or
        # in different places in this codebase and allow to inject functionality for ranges and masks
        if library in LIBRARIES["awkward"]:
            if how == dict:
                return dict_of_arrays
            elif how == list:
                return [value for value in dict_of_arrays.values()]
            elif how == tuple:
                return tuple(value for value in dict_of_arrays.values())

        if library in LIBRARIES["pandas"]:
            return Uproot4Methods.arraydict_to_pandas(dict_of_arrays)

    def arrays(self, expressions, *args, **kwargs):
        if "outputtype" in kwargs:
            # renamed uproot3 -> uproot4
            outputtype = kwargs.pop("outputtype")
            kwargs["how"] = outputtype

        operations = kwargs.get("operations", [])
        tree_arrays = self.array_dict(keys=expressions)
        for operation in operations:
            for key, value in tree_arrays.items():
                tree_arrays[key] = operation(value)
        return self.array_exporter(tree_arrays, **kwargs)

    def array(self, key):
        return self[key]

    def evaluate(self, expression, **kwargs):
        return ak.numexpr.evaluate(expression, self, **kwargs)

    @staticmethod
    def counts(array, **kwargs):
        axis = kwargs.pop("axis", 1)
        return ak.count(Uproot4Methods.only_valid_entries(array), axis=axis, **kwargs)

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
    def arrays_as_np_array(data, array_names, **kwargs):
        """
        Takes input data and converts it to an array of numpy arrays.
        e.g. arrays_as_np_array(data, ["x", "y"])
        results in
        array(<array of x>, <array of y>)
        """
        return data.arrays(
            array_names,
            library="ak",
            outputtype=list,
            **kwargs,
        )

    @staticmethod
    def to_pandas(data, keys):
        return data.arrays(
            keys,
            library="pd",
        )

    @staticmethod
    def valid_entry_mask(data: TreeLike) -> ak.Array:
        return ~ak.is_none(data)

    @staticmethod
    def only_valid_entries(data: TreeLike) -> ak.Array:
        return data[Uproot4Methods.valid_entry_mask(data)]

    @staticmethod
    def filtered_len(data: TreeLike) -> int:
        return len(data[~ak.is_none(data)])

    @staticmethod
    def fill_none(data: ArrayLike, fill_value, **kwargs):
        axis = kwargs.pop("axis", None)
        return ak.fill_none(array=data, value=fill_value, axis=axis, **kwargs)

    @staticmethod
    def values_as_type(data: ArrayLike, dtype, **kwargs):
        return ak.values_astype(data, dtype, **kwargs)


ArrayMethods = Uproot4Methods


class TreeToDictAdaptorV0(Uproot3Methods, TreeToDictAdaptor):
    pass


class TreeToDictAdaptorV1(Uproot4Methods, TreeToDictAdaptor):
    pass


register("uproot3", TreeToDictAdaptorV0)
register("uproot4", TreeToDictAdaptorV1)


# class ApplyRange(Callable):
#     def __init__(self, range):
#         self.range = range

#     def __call__(self, arrays):
#         pass

# class ApplyMask(Callable):
#     def __init__(self, mask):
#         self.mask = mask

#     def __call__(self, arrays):
#         pass

class Ranger(object):
    """
        TODO: range is just a different way of indexing --> refactor
    """
    tree: TreeToDictAdaptor
    start: int
    stop: int
    block_size: int
    mask: Any

    def __init__(self, tree: TreeToDictAdaptor, start: int, stop: int) -> None:
        self.tree = tree
        self.start = start
        self.stop = stop

        tree_size = tree.num_entries
        self.block_size = stop - start if stop > start > 0 else tree_size
        self.mask = np.ones(tree_size, dtype=bool)
        if self.block_size < tree_size:
            self.mask = np.zeros(tree_size, dtype=bool)
            self.mask[start:stop] = True

    @property
    def num_entries(self) -> int:
        """Returns the size of the range - overwrites tree.num_entries."""
        return self.block_size

    @property
    def unfiltered_num_entries(self) -> int:
        return self.tree.num_entries

    def __getitem__(self, key):
        return ak.mask(self.tree[key], self.mask)

    def __setitem__(self, key, value):
        # self.tree[key][self.start:self.stop] = value[self.start:self.stop]
        self.tree[key] = value

    def __delitem__(self, key):
        del self.tree[key]

    def __contains__(self, key):
        return key in self.tree

    def __len__(self):
        return self.block_size

    def array(self, key):
        return self[key]

    def arrays(self, *args, **kwargs):
        operations = kwargs.pop("operations", [])
        operations.append(lambda x: ak.mask(x, self.mask))
        kwargs["operations"] = operations
        arrays = self.tree.arrays(*args, **kwargs)
        return arrays

    def new_variable(self, name, value):
        import awkward as ak
        if len(value) < self.tree.num_entries:
            new_value = ak.concatenate(
                [
                    ak.Array([None] * self.start),
                    value,
                    ak.Array([None] * (self.tree.num_entries - self.stop))
                ],
                axis=0
            )
        else:
            new_value = value
        assert len(new_value) == self.tree.num_entries
        self.tree.new_variable(name, new_value, context=self.tree)

    def evaluate(self, expression, **kwargs):
        import awkward as ak
        return ak.numexpr.evaluate(expression, self, **kwargs)

    def keys(self):
        return self.tree.keys()

    def arrays_to_pandas(self, *args, **kwargs):
        return self.tree.arrays_to_pandas(*args, **kwargs)


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
        self._mask = np.ones(tree.num_entries, dtype=bool) if mask is None else mask

        if mask is not None and len(mask) < tree.unfiltered_num_entries:
            self._mask = ak.concatenate(
                [
                    ak.Array([False] * tree.start),
                    self._mask,
                    ak.Array([False] * (tree.unfiltered_num_entries - tree.stop))
                ],
                axis=0
            )

    def __getitem__(self, key):
        if self._mask is None:
            return self._tree[key]
        try:
            if len(self._mask) > len(self._tree):
                return self._tree[key][self._tree.start:self._tree.stop].mask[self._mask]
        except TypeError as e:
            raise e
        return self._tree[key].mask[self._mask]

    def __len__(self):
        return len(self._tree)

    def __contains__(self, key):
        return key in self._tree

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

    def reset_mask(self):
        self._mask = None

    def reset_cache(self):
        pass

    def array(self, key):
        return self[key]

    def arrays(self, *args, **kwargs):
        operations = kwargs.pop("operations", [])
        if self._mask is not None:
            operations.append(lambda x: ak.mask(x, self._mask))

        kwargs["operations"] = operations
        arrays = self._tree.arrays(*args, **kwargs)
        return arrays

    def evaluate(self, expression, **kwargs):
        import awkward as ak
        return ak.numexpr.evaluate(expression, self, **kwargs)

    def keys(self):
        return self._tree.keys()

    def new_variable(self, name, value):
        self._tree.new_variable(name, value)


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
