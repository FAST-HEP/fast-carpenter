import numpy as np
import awkward as ak

from functools import partial

CONSTANTS = {
    "nan": np.nan,
    "inf": np.inf,
    "pi": np.pi,
    "e": np.e,
}


def _slice(x, slice_args):
    return x[slice(*slice_args)]


def _constant(x):
    if x in CONSTANTS:
        return CONSTANTS[x]
    return x


# we need this to support either one awkward array or lists of awkward arrays
# pairs([Muon_E, Muon_Px, Muon_Py, Muon_Pz]) -> pairs of muon 4-vectors
# pairs(Muon_Pt) -> pairs of muon pT
# def pairs(collection: Iterable) -> Iterable:
#     """Yield pairs of elements from a collection."""
#     return ak.combinations(collection, 2)


SUPPORTED_FUNCTIONS={
    "add": ak.Array.__add__,
    "eval": ak._connect.numexpr.evaluate,
    "sqrt": np.sqrt,
    "slice": _slice,
    "constant": _constant,
    "count_nonzero": partial(ak.count_nonzero, axis=1),
}
