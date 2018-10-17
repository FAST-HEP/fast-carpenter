import uproot
from atuproot.BEvents import BEvents
from .masked_tree import MaskedUprootTree
from .tree_wrapper import WrappedTree


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

        events = BEvents(MaskedUprootTree(WrappedTree(tree)),
                         self.config.nevents_per_block,
                         self.config.start_block,
                         self.config.stop_block)
        events.config = self.config
        return events
