import uproot
from atuproot.BEvents import BEvents
from .masked_tree import MaskedUprootTree
from .tree_wrapper import WrappedTree


class EventRanger():
    def __init__(self):
        self._owner = None

    def set_owner(self, owner):
        self._owner = owner

    @property
    def start_entry(self):
        return (self._owner.start_block + self._owner.iblock) * self._owner.nevents_per_block

    @property
    def stop_entry(self):
        i_block = min(self._owner.iblock + 1, self._owner.nblocks)
        stop_entry = (self._owner.start_block + i_block) * self._owner.nevents_per_block
        return min(self._owner.nevents_in_tree, stop_entry)

    @property
    def entries_in_block(self):
        if self._owner and self._owner.iblock > -1:
            return self.stop_entry - self.start_entry
        return None


class EventBuilder(object):
    def __init__(self, config):
        self.config = config

    def __repr__(self):
        return '{}({!r})'.format(
            self.__class__.__name__,
            self.config,
        )

    def __call__(self):
        if len(self.config.inputPaths) != 1:
            # TODO - support multiple inputPaths
            raise AttributeError("Multiple inputPaths not yet supported")

        # Try to open the tree - some machines have configured limitations
        # which prevent memmaps from begin created. Use a fallback - the
        # localsource option
        try:
            rootfile = uproot.open(self.config.inputPaths[0])
            tree = rootfile[self.config.treeName]
        except MemoryError:
            rootfile = uproot.open(self.config.inputPaths[0],
                                   localsource=uproot.FileSource.defaults)
            tree = rootfile[self.config.treeName]


        ranges = EventRanger()
        events = BEvents(MaskedUprootTree(WrappedTree(tree, ranges)),
                         self.config.nevents_per_block,
                         self.config.start_block,
                         self.config.stop_block)
        events.config = self.config
        ranges.set_owner(events)
        return events
