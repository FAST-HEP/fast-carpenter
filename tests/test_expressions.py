import pytest
import uproot
from fast_carpenter import expressions


@pytest.fixture
def infile():
    filename = "tests/data/CMS_HEP_tutorial_ww.root"
    return uproot.open(filename)["events"]


def test_get_branches(infile):
    valid = infile.allkeys()

    cut = "NMuon > 1"
    branches = expressions.get_branches(cut, valid)
    assert branches == ["NMuon"]

    cut = "NMuon_not_found > 1 and NElectron > 3"
    branches = expressions.get_branches(cut, valid)
    assert branches == ["NElectron"]
