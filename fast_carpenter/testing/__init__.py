from collections import namedtuple
import numpy as np

FakeEventRange = namedtuple("FakeEventRange", "start_entry stop_entry entries_in_block")


class Namespace():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeBEEvent(object):
    def __init__(self, tree, eventtype):
        self.tree = tree
        self.config = Namespace(dataset=Namespace(eventtype=eventtype))

    def __len__(self):
        return len(self.tree)

    def count_nonzero(self):
        return self.tree.count_nonzero()


class FakeTree(dict):
    length: int = 101

    def __init__(self, length=101):
        super(FakeTree, self).__init__(
            NMuon=np.linspace(0, 5., length),
            NElectron=np.linspace(0, 10, length),
            NJet=np.linspace(2, -18, length),
        )
        self.length = length

    def __len__(self):
        return self.length

    def arrays(self, array_names, library="np", outputtype=list):
        return [self[name] for name in array_names]
