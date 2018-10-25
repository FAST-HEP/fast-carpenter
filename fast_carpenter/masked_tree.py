import numpy as np


class MaskedUprootTree():
    def __init__(self, tree, mask=None):
        if isinstance(tree, MaskedUprootTree):
            self.tree = tree.tree
            self._mask = tree._mask
        else:
            self.tree = tree
            self._mask = mask

        if mask is None:
            return
        if isinstance(mask, (tuple, list)):
            mask = np.array(mask)
        elif not isinstance(mask, np.ndarray):
            raise RuntimeError("mask is not a numpy array, a list, or a tuple")

        if np.issubdtype(mask.dtype, np.integer):
            self._mask = mask
        elif mask.dtype.kind == "b":
            if len(mask) != len(tree):
                raise RuntimeError("boolean mask has a different length to the input tree")
            self._mask = np.where(mask)[0]

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
            self._mask = new_mask
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
