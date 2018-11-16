"""
This has to be what is probably the hackiest piece of code I've ever written.
It's very tightly coupled to uproot, and just by importing it will change the
way uproot works.  However, it allows me to achieve the functionality of adding
a branch to uproot trees with no changes to actual code in uproot and with
minimal coding on my side...
"""
import uproot
from uproot.interp.jagged import asjagged
import copy
import awkward


def wrapped_interpret(branch, **kwargs):
    from uproot.interp.auto import interpret
    result = interpret(branch, **kwargs)
    if result:
        return result

    if isinstance(branch, WrappedTree.FakeBranch):
        if isinstance(branch._values, awkward.JaggedArray):
            return asjagged(branch._values.content)
        else:
            return branch._values

    return None


uproot.tree.interpret = wrapped_interpret


class WrappedTree(object):
    def __init__(self, tree, event_ranger):
        self.tree = copy.copy(tree)
        self.tree.old_itervalues = self.tree.itervalues
        self.tree.itervalues = self.itervalues
        self.tree.old_arrays = self.tree.arrays
        self.tree.arrays = self.arrays
        self.tree.old_array = self.tree.array
        self.tree.array = self.array
        self.event_ranger = event_ranger
        self.reset_cache()

    def itervalues(self, *args, **kwargs):
        for array in self.extras.values():
            yield array
        for vals in self.tree.old_itervalues(*args, **kwargs):
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

    class pandas_wrap():
        def __init__(self, owner):
            self._owner = owner

        def df(self, *args, **kwargs):
            self._owner.update_array_args(kwargs)
            df = self._owner.tree.pandas.df(*args, **kwargs)
            return df

    @property
    def pandas(self):
        return WrappedTree.pandas_wrap(self)

    class FakeBranch(object):
        def __init__(self, name, values, event_ranger):
            self.name = name
            self._values = values
            self._fLeaves = []
            self.fLeaves = []
            self.event_ranger = event_ranger

        def array(self, entrystart=None, entrystop=None, blocking=True, **kws):
            array = self._values
            if entrystart:
                entrystart -= self.event_ranger.start_entry
            if entrystop:
                entrystop -= self.event_ranger.start_entry

            def wait():
                values = array[entrystart:entrystop]
                return values

            if not blocking:
                return wait
            return wait()

        def __getattr__(self, attr):
            return getattr(self._values, attr)

        def __len__(self):
            return len(self._values)

    def new_variable(self, name, value):
        if len(value) != len(self):
            msg = "New array %s does not have the right length: %d not %d"
            raise ValueError(msg % (name, len(value), len(self)))

        outputtype = WrappedTree.FakeBranch
        self.extras[name] = outputtype(name, value, self.event_ranger)

    def __getattr__(self, attr):
        return getattr(self.tree, attr)

    def __len__(self):
        chunk_size = self.event_ranger.entries_in_block
        if chunk_size:
            return chunk_size
        return len(self.tree)

    def reset_cache(self):
        self.branch_cache = {}
        self.extras = {}
