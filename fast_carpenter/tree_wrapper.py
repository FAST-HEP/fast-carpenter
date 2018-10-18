"""
This has to be what is probably the hackiest piece of code I've ever written.
It's very tightly coupled to uproot, and just by importing it will change the
way uproot works.  However, it allows me to achieve the functionality of adding
a branch to uproot trees with no changes to actual code in uproot and with
minimal coding on my side...
"""
import uproot
from uproot.interp.auto import interpret
from uproot.interp.jagged import asjagged
import copy
import awkward


def wrapped_interpret(branch, **kwargs):
    result = interpret(branch, **kwargs)
    if result:
        return result

    if isinstance(branch, WrappedTree.FakeBranch):
        return (branch, branch._values)

    return None


uproot.tree.interpret = wrapped_interpret


class WrappedTree(object):
    def __init__(self, tree):
        self.tree = copy.copy(tree)
        self.extras = {}
        self.tree.old_itervalues = self.tree.itervalues
        self.tree.itervalues = self.itervalues
        self.tree.old_arrays = self.tree.arrays
        self.tree.arrays = self.arrays
        self.branch_cache = {}

    def itervalues(self, *args, **kwargs):
        for array in self.extras.values():
            yield array
        for vals in self.tree.old_itervalues(*args, **kwargs):
            yield vals

    def arrays(self, *args, **kwargs):
        kwargs.setdefault("cache", self.branch_cache)
        return self.tree.old_arrays(*args, **kwargs)

    class FakeBranch(object):
        def __init__(self, name, values):
            self.name = name
            self._values = values
            if isinstance(values, (awkward.JaggedArray, asjagged)):
                self._values = values.content
            self._fLeaves = []
            self.fLeaves = []

        def array(self, entrystart=None, entrystop=None, blocking=True, **kws):
            array = self._values

            if not blocking:
                return lambda: array[entrystart:entrystop]
            return array[entrystart:entrystop]

        def __getattr__(self, attr):
            return getattr(self._values, attr)

        def __len__(self):
            return len(self._values)

#    class FakeBranchJagged(asjagged):
#        def __init__(self, name, values):
#            self.name = name
#            self._values = values
#            self._fLeaves = []
#
#        def array(self, entrystart=None, entrystop=None, blocking=True, flatten=False, **kws):
#            if flatten:
#                array = self._values.content
#            else:
#                array = self._values
#
#            if not blocking:
#                return lambda: array[entrystart:entrystop]
#            return array[entrystart:entrystop]
#
#        def __getattr__(self, attr):
#            return getattr(self._values, attr)
#
#        def __len__(self):
#            return len(self._values)

    def new_variable(self, name, value):
        if isinstance(value, awkward.JaggedArray):
            length = len(value.content)
            outputtype = WrappedTree.FakeBranch
        else:
            length = len(value)
            outputtype = WrappedTree.FakeBranch
        if length != len(self.tree):
            msg = "New array %s does not have the right length: %d not %d"
            raise ValueError(msg % (name, length, len(self.tree)))
        self.extras[name] = outputtype(name, value)

    def __getattr__(self, attr):
        return getattr(self.tree, attr)

    def __len__(self):
        return len(self.tree)
