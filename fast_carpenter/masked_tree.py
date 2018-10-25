import numpy as np


class MaskedUprootTree():
    def __init__(self, tree, mask=None):
        if isinstance(tree, MaskedUprootTree):
            self.tree = tree.tree
            self._mask = tree._mask
            return

        self.tree = tree

        if mask is None:
            self._mask = None
            return

        self._mask = _normalise_mask(mask, len(self.tree))

    class pandas_wrap():
        def __init__(self, owner):
            self._owner = owner

        def df(self, *args, **kwargs):
            df = self._owner.tree.pandas.df(*args, **kwargs)
            if self._owner._mask is None:
                return df
            masked = df.loc[self._owner._mask]
            return masked

    @property
    def pandas(self):
        return MaskedUprootTree.pandas_wrap(self)

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
