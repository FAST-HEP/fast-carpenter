import pytest
import numpy as np
import fast_carpenter.backends.alphatwirl_event_builder as builder


@pytest.fixture
def wrapped_be(infile):
    return builder.BEventsWrapped(infile, nevents_per_block=1000)


def test_contains(wrapped_be):
    assert "Muon_Py" in wrapped_be.tree
    assert "not_a_branch" not in wrapped_be.tree


def test_block_sizes(wrapped_be):
    block_lengths = np.array([len(wrapped_be.tree) for _ in wrapped_be])
    assert all(block_lengths[:-1] == 1000)
    assert block_lengths[-1] == 580
    assert len(block_lengths) == len(wrapped_be)
    assert len(block_lengths) == 5
