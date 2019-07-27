import pandas as pd
import numpy as np
from .tree_wrapper import WrappedTree


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
            masked = mask_df(df, self._owner._mask, self._owner.event_ranger.start_entry)
            return masked

    @property
    def pandas(self):
        return MaskedUprootTree.PandasWrap(self)

    def unmasked_array(self, *args, **kwargs):
        return self.tree.array(*args, **kwargs)

    def unmasked_arrays(self, *args, **kwargs):
        return self.tree.arrays(*args, **kwargs)

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
            raise RuntimeError("boolean mask has a different length to the input tree")
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
