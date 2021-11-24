"""
This has to be what is probably the hackiest piece of code I've ever written.
It's very tightly coupled to uproot, and just by importing it will change the
way uproot works.  However, it allows me to achieve the functionality of adding
a branch to uproot trees with no changes to actual code in uproot and with
minimal coding on my side...
"""
import copy

from .uproot_layer import add_new_variable, FakeBranch


class WrappedTree(object):

    def __init__(self, tree, event_ranger):
        self.tree = copy.copy(tree)
        self.tree.old_itervalues = self.tree.itervalues
        self.tree.old_iteritems = self.tree.iteritems
        self.tree.itervalues = self.itervalues
        self.tree.iteritems = self.iteritems
        self.tree.old_arrays = self.tree.arrays
        self.tree.arrays = self.arrays
        self.tree.old_array = self.tree.array
        self.tree.array = self.array
        self.event_ranger = event_ranger
        self.branch_cache = {}
        self.extras = {}

    def itervalues(self, *args, **kwargs):
        for vals in self.tree.old_itervalues(*args, **kwargs):
            yield vals

    def iteritems(self, *args, **kwargs):
        for array in self.extras.values():
            yield array.name, array
        for vals in self.tree.old_iteritems(*args, **kwargs):
            yield vals

    def arrays(self, *args, **kwargs):
        self.update_array_args(kwargs)
        return self.tree.old_arrays(*args, **kwargs)

    def array(self, *args, **kwargs):
        self.update_array_args(kwargs)
        return self.tree.old_array(*args, **kwargs)

    def update_array_args(self, kwargs):
        kwargs.setdefault("cache", self.branch_cache)
        kwargs.setdefault("entrystart", self.event_ranger.start_entry)
        kwargs.setdefault("entrystop", self.event_ranger.stop_entry)

    class PandasWrap():
        def __init__(self, owner):
            self._owner = owner

        def df(self, *args, **kwargs):
            self._owner.update_array_args(kwargs)
            df = self._owner.tree.pandas.df(*args, **kwargs)
            return df

    @property
    def pandas(self):
        return WrappedTree.PandasWrap(self)

    def new_variable(self, name, value):
        if name in self:
            msg = "Trying to overwrite existing variable: '%s'"
            raise ValueError(msg % name)
        if len(value) != len(self):
            msg = "New array %s does not have the right length: %d not %d"
            raise ValueError(msg % (name, len(value), len(self)))

        outputtype = FakeBranch
        name = add_new_variable(name)

        self.extras[name] = outputtype(name, value, self.event_ranger)

    def __getattr__(self, attr):
        return getattr(self.tree, attr)

    def __contains__(self, element):
        return self.tree.__contains__(element)

    def __len__(self):
        chunk_size = self.event_ranger.entries_in_block
        if chunk_size:
            return chunk_size
        return len(self.tree)

    def reset_cache(self):
        self.branch_cache.clear()
        for k in self.extras.keys():
            if k in self.tree._branchlookup:
                del self.tree._branchlookup[k]
        self.extras.clear()
