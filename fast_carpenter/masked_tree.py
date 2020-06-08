from types import SimpleNamespace

import pandas as pd
import numpy as np
from .tree_wrapper import WrappedTree
from .dataspace import recursive_index


class MaskedUprootTree(object):
    def __init__(self, tree, event_ranger, mask=None):
        if isinstance(tree, MaskedUprootTree):
            self.tree = tree.tree
            self._mask = tree._mask
            return

        self.tree = WrappedTree(tree, event_ranger)
        self.event_ranger = event_ranger

        if mask is None:
            self._mask = None
            return

        self._mask = _normalise_mask(mask, len(self.tree))

    class PandasWrap():
        def __init__(self, owner):
            self._owner = owner

        def df(self, *args, **kwargs):
            df = self._owner.tree.pandas.df(*args, **kwargs)
            if self._owner._mask is None:
                return df
            masked = mask_df(df, self._owner._mask,
                             self._owner.event_ranger.start_entry)
            return masked

    @property
    def pandas(self):
        return MaskedUprootTree.PandasWrap(self)

    def unmasked_array(self, *args, **kwargs):
        return self.tree.array(*args, **kwargs)

    def unmasked_arrays(self, *args, **kwargs):
        return self.tree.arrays(*args, **kwargs)

    def __getitem__(self, key):
        return self.tree[key]

    def array(self, *args, **kwargs):
        array = self.tree.array(*args, **kwargs)
        if self._mask is None:
            return array
        return array[self._mask]

    def arrays(self, *args, **kwargs):
        arrays = self.tree.arrays(*args, **kwargs)
        if self._mask is None:
            return arrays
        if isinstance(arrays, dict):
            return {k: v[self._mask] for k, v in arrays.items()}
        if isinstance(arrays, tuple):
            return tuple([v[self._mask] for v in arrays])
        if isinstance(arrays, list):
            return [v[self._mask] for v in arrays]
        if isinstance(arrays, pd.DataFrame):
            return mask_df(arrays, self._mask, self.event_ranger.start_entry)
        if isinstance(arrays, np.ndarray):
            if arrays.ndim == 1:
                return arrays[self._mask]
            if arrays.ndim == 2:
                if arrays.shape[1] == len(self.tree):
                    return arrays[:, self._mask]
            msg = "Unexpected numpy array for mask, shape:%s, mask length: %s"
            raise NotImplementedError(msg % (arrays.shape, len(self)))
        return arrays[self._mask]

    @property
    def mask(self):
        return self._mask

    def apply_mask(self, new_mask):
        if self._mask is None:
            self._mask = _normalise_mask(new_mask, len(self.tree))
        else:
            self._mask = self._mask[new_mask]

    def __len__(self):
        if self._mask is None:
            return len(self.tree)
        return len(self._mask)

    def __contains__(self, element):
        return self.tree.__contains__(element)

    def __getattr__(self, attr):
        return getattr(self.tree, attr)

    def reset_mask(self):
        self._mask = None


def _normalise_mask(mask, tree_length):
    if isinstance(mask, (tuple, list)):
        mask = np.array(mask)
    elif not isinstance(mask, np.ndarray):
        raise RuntimeError("mask is not a numpy array, a list, or a tuple")

    if np.issubdtype(mask.dtype, np.integer):
        return mask
    elif mask.dtype.kind == "b":
        if len(mask) != tree_length:
            raise RuntimeError(
                "boolean mask has a different length to the input tree")
        return np.where(mask)[0]


def mask_df(df, mask, start_event):
    mask = mask + start_event

    # Either of these methods could work on a general df (multiindex, or not) but they
    # have opposite performances, so check if multi-index and choose accordingly
    if isinstance(df.index, pd.MultiIndex):
        broadcast_map = np.isin(df.index.get_level_values("entry"), mask)
        masked = df.iloc[broadcast_map]
    else:
        masked = df.loc[mask]
    return masked


class MaskedTrees(object):
    nonOverloadedFunctions = [
        'pandas', 'array', 'arrays', 'mask', 'unmasked_array',
        'unmasked_arrays', 'apply_mask', 'trees', '_mask', 'event_ranger',
        '_index', '__getitem__',
    ]

    def __init__(self, trees, event_ranger, mask=None):
        self.pandas = SimpleNamespace(df=self._df)
        self.trees = {}
        self.provenance = {}
        self._mask = mask
        self.event_ranger = event_ranger
        self._index = {}
        for name, tree in trees.items():
            self.trees[name] = MaskedUprootTree(tree, event_ranger, mask)
            self.provenance[name] = name.split('/') if '/' in name else [name]
            self._index.update(recursive_index(self._index, self.provenance[name], self.trees[name]))

    def __getitem__(self, key):
        return self._index[key]

    def _df(self, *args, **Kwargs):
        dfs = []
        for name, tree in self.trees.items():
            df = MaskedUprootTree.PandasWrap(tree).df(*args, **Kwargs)
            df.columns = ["{}.".format(name) + str(col) for col in df.columns]
            dfs.append(df)
        return pd.concat(dfs, axis=1)

    def unmasked_array(self, *args, **kwargs):
        return self.tree.array(*args, **kwargs)

    def unmasked_arrays(self, *args, **kwargs):
        return self.tree.arrays(*args, **kwargs)

    def array(self, *args, **kwargs):
        array, exception = None, None
        try:
            value = self._index[args[0]]
            array = value.array(*args[1:], **kwargs)
        except Exception as e:
            print(self._index)
            exception = e
        if array is None:
            raise exception

        if self._mask is None:
            return array
        return array[self._mask]

    def arrays(self, *args, **kwargs):
        outputtype = kwargs.get('outputtype', dict)

        arrays = list()
        for name, tree in self.trees.items():
            treeArrays = tree.arrays(*args, **kwargs)
            outputtype = type(treeArrays)
            arrays.append((name, treeArrays))

        if outputtype is dict:
            return {'{}.{}'.format(name, aName): value for (name, treeArray) in arrays for aName, value in treeArray.items()}
        if outputtype is list:
            return [value for (name, treeArray) in arrays for value in treeArray]
        if outputtype is tuple:
            return tuple([value for (name, treeArray) in arrays for value in treeArray])
        if outputtype is pd.DataFrame:
            dfs = []
            for (name, treeArrays) in arrays:
                df = treeArrays
                df.columns = ["{}.".format(name) + str(col)
                              for col in df.columns]
                dfs.append(df)
            # return mask_df(pd.concat(dfs, axis=1), self._mask, self.event_ranger.start_entry)
            return pd.concat(dfs, axis=1)
        if outputtype is np.ndarray and self._mask is not None:
            results = [value for (_, value) in arrays]
            return np.array(results)

        return arrays

    @property
    def mask(self):
        return self._mask

    def apply_mask(self, new_mask):
        for tree in self.trees.values():
            tree.apply_mask(new_mask)
            self._mask = tree._mask

    def __len__(self):
        # if self._mask is None:
        return max([len(e) for e in self.trees.values()])
        # return len(self._mask)

    def __contains__(self, element):
        # TODO: return a masked wrapper instead
        return element in self._index

    def __getattr__(self, attr):
        if attr in MaskedTrees.nonOverloadedFunctions:
            return self.__getattribute__(attr)
        for tree in self.trees.values():
            if tree.__contains__(attr):
                return getattr(tree, attr)
        raise AttributeError('Cannot find attribute "{}"'.format(attr))

    def reset_mask(self):
        self._mask = None
