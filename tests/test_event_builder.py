import pytest
import numpy as np
import fast_carpenter.event_builder as builder


@pytest.fixture
def wrapped_BE(infile):
    return builder.BEventsWrapped(infile, nevents_per_block=1000)


def test_contains(wrapped_BE):
    assert "Muon_Py" in wrapped_BE.tree
    assert "not_a_branch" not in wrapped_BE.tree


def test_block_sizes(wrapped_BE):
    block_lengths = np.array([len(wrapped_BE.tree) for _ in wrapped_BE])
    assert all(block_lengths[:-1] == 1000)
    assert block_lengths[-1] == 580
    assert len(block_lengths) == len(wrapped_BE)
    assert len(block_lengths) == 5
