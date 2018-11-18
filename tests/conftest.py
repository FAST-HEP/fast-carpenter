import pytest
import uproot


@pytest.fixture()
def infile():
    filename = "tests/data/CMS_HEP_tutorial_ww.root"
    return uproot.open(filename)["events"]


class Namespace():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeBEEvent(object):
    def __init__(self, tree, eventtype):
        self.tree = tree
        self.config = Namespace(dataset=Namespace(eventtype=eventtype))
