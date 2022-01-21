
import awkward as ak
import numpy as np

def test_compare_np_to_ak(uproot4_tree):
    """ see https://github.com/scikit-hep/awkward-1.0/issues/1241 """
    with open('tests/data/eventweights.npy', 'rb') as f:
        np_array = np.load(f)
    
    np_sum = np.sum(np_array)
    for axis in [None, -1, 0]:
        ak_sum = ak.sum(np_array, axis=axis)
        assert ak.sum(np_array, axis=axis) == np_sum

    
