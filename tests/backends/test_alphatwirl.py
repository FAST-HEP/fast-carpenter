import pytest
import fast_carpenter.backends._alphatwirl as builder


@pytest.fixture
def wrapped_be(uproot4_tree):
    """ Not going to be used for uproot4 """
    return builder.BEventsWrapped(uproot4_tree, nevents_per_block=1000)


def test_contains(wrapped_be):
    assert "Muon_Py" in wrapped_be.tree
    assert "not_a_branch" not in wrapped_be.tree
