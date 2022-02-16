from typing import Any, List

import awkward as ak
import numpy as np

from .tree_adapter import ArrayMethods


def extract_weights(data: Any, weight_names: List[str]) -> np.ndarray:
    return ArrayMethods.arrays_as_np_array(data, weight_names)


def get_unweighted_increment(weights: Any, mask: Any) -> int:
    """
        Returns the total number of unweighted events.
        If no mask is present, this should be the smallest dimenion of weights.
        If a mask is present, this should be the number of events passing the mask (i.e. non-zero entries).
    """
    if mask is None:
        return min([len(w) for w in weights])
    elif ArrayMethods.is_bool(mask):
        return np.count_nonzero(mask)
    else:
        return len(mask)


def get_weighted_increment(weights: np.ndarray, mask: Any) -> np.ndarray:
    """
        Returns the total number of weighted events per weight category.
        If no mask is present, this will sum up all individual weights.
        If a mask is present, the sum is only performed over the entries that are not masked.
    """
    if mask is not None:
        weight_mask = [mask] * len(weights)  # repeat mask for each weight
        weights = ak.mask(weights, weight_mask)
    return ArrayMethods.sum(weights, axis=1)
