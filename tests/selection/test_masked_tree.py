from __future__ import division
import pytest
import numpy as np
import uproot
import fast_chainsaw.selection.masked_tree as m_tree


@pytest.fixture
def infile():
    filename = "tests/data/CMS_HEP_tutorial_ww.root"
    return uproot.open(filename)["events"]


@pytest.fixture
def tree_no_mask(infile):
    return m_tree.MaskedUprootTree(infile)


@pytest.fixture
def tree_w_mask_bool(infile):
    n_events = len(infile)
    mask = np.ones(n_events, dtype=bool)
    mask[::2] = False
    return m_tree.MaskedUprootTree(infile, mask)


@pytest.fixture
def tree_w_mask_int(infile):
    n_events = len(infile)
    mask = np.ones(n_events, dtype=bool)
    mask[::2] = False
    mask = np.where(mask)[0]
    return m_tree.MaskedUprootTree(infile, mask)


def test_no_mask(tree_no_mask, infile):
    assert len(tree_no_mask) == len(infile)
    assert np.all(tree_no_mask.mask == np.arange(len(infile)))
    assert np.all(tree_no_mask.pandas.df("EventWeight") == infile.pandas.df("EventWeight"))


def test_w_mask_bool(tree_w_mask_bool, infile):
    assert len(tree_w_mask_bool) == len(infile) // 2
    df = tree_w_mask_bool.pandas.df("NMuon")
    assert len(df) == len(infile) // 2
    new_mask = np.ones(len(tree_w_mask_bool), dtype=bool)
    new_mask[::2] = False
    tree_w_mask_bool.apply_mask(new_mask)
    assert len(tree_w_mask_bool) == len(infile) // 4


def test_w_mask_int(tree_w_mask_int, infile):
    assert len(tree_w_mask_int) == len(infile) // 2
    tree_w_mask_int.apply_mask(np.arange(0, len(tree_w_mask_int), 2))
    assert len(tree_w_mask_int) == len(infile) // 4
