from __future__ import division
import pytest
import numpy as np
import pandas as pd
import fast_carpenter.masked_tree as m_tree


@pytest.fixture
def tree_no_mask(infile, full_event_range):
    return m_tree.MaskedUprootTree(infile, event_ranger=full_event_range)


@pytest.fixture
def tree_w_mask_bool(infile, event_range):
    mask = np.ones(event_range.entries_in_block, dtype=bool)
    mask[::2] = False
    return m_tree.MaskedUprootTree(infile, event_ranger=event_range, mask=mask)


@pytest.fixture
def tree_w_mask_int(infile, event_range):
    mask = np.ones(event_range.entries_in_block, dtype=bool)
    mask[::2] = False
    mask = np.where(mask)[0]
    return m_tree.MaskedUprootTree(infile, event_ranger=event_range, mask=mask)


def test_no_mask(tree_no_mask, infile):
    assert len(tree_no_mask) == len(infile)
    assert tree_no_mask.mask is None
    assert np.all(tree_no_mask.pandas.df("EventWeight") == infile.pandas.df("EventWeight"))


def test_w_mask_bool(tree_w_mask_bool, infile):
    assert len(tree_w_mask_bool) == 50
    df = tree_w_mask_bool.pandas.df("NMuon")
    assert len(df) == 50
    new_mask = np.ones(len(tree_w_mask_bool), dtype=bool)
    new_mask[::2] = False
    tree_w_mask_bool.apply_mask(new_mask)
    assert len(tree_w_mask_bool) == 25


def test_w_mask_int(tree_w_mask_int, infile):
    assert len(tree_w_mask_int) == 50
    tree_w_mask_int.apply_mask(np.arange(0, len(tree_w_mask_int), 2))
    assert len(tree_w_mask_int) == 25
    df = tree_w_mask_int.pandas.df("Muon_Px")
    assert len(df.index.unique(0)) == 25


def test_array(tree_w_mask_int, infile):
    assert len(tree_w_mask_int) == 50
    tree_w_mask_int.apply_mask(np.arange(0, len(tree_w_mask_int), 2))
    assert len(tree_w_mask_int) == 25
    array = tree_w_mask_int.array("Muon_Px")
    assert len(array) == 25


def test_arrays(tree_w_mask_int, infile):
    assert len(tree_w_mask_int) == 50
    tree_w_mask_int.apply_mask(np.arange(0, len(tree_w_mask_int), 2))
    assert len(tree_w_mask_int) == 25

    arrays = tree_w_mask_int.arrays(["Muon_Px", "Muon_Py"], outputtype=dict)
    assert isinstance(arrays, dict)
    assert len(arrays) == 2
    assert len(arrays["Muon_Py"]) == 25
    assert len(arrays["Muon_Px"]) == 25

    for outtype in [list, tuple]:
        arrays = tree_w_mask_int.arrays(["Muon_Px", "Muon_Py"], outputtype=outtype)
        assert isinstance(arrays, outtype)
        assert len(arrays) == 2
        assert len(arrays[0]) == 25
        assert len(arrays[1]) == 25

    arrays = tree_w_mask_int.arrays(["Muon_Px", "Muon_Py"],
                                    outputtype=lambda *args: np.array(args))
    assert isinstance(arrays, np.ndarray)
    assert arrays.shape == (2, 25)

    arrays = tree_w_mask_int.arrays(["Muon_Px"],
                                    outputtype=lambda *args: np.array(args))
    assert isinstance(arrays, np.ndarray)
    assert arrays.shape == (1, 25)

    arrays = tree_w_mask_int.arrays(["Muon_Px", "Muon_Py"],
                                    outputtype=pd.DataFrame)
    assert isinstance(arrays, pd.DataFrame)
    assert len(arrays) == 25
    assert len(arrays.columns) == 2

