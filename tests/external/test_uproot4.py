import uproot
import numpy as np

def test_library(uproot4_tree):
    muon_py, muon_pz = uproot4_tree.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    arrays = {
        "Muon_Py": muon_py,
        "Muon_Pz": muon_pz,
    }
    arrays["Muon_momentum"] = np.hypot(muon_py, muon_pz)

    library = uproot.interpretation.library._regularize_library("ak")
    how = tuple

    expression_context = [(key, key) for key in arrays]
    assert library.group(arrays, expression_context, how=how) == (muon_py, muon_pz, arrays["Muon_momentum"])


def test_library_with_dict(uproot4_tree):
    muon_py, muon_pz = uproot4_tree.arrays(["Muon_Py", "Muon_Pz"], how=tuple)
    arrays = {
        "Muon_Py": muon_py,
        "Muon_Pz": muon_pz,
    }
    arrays["Muon_momentum"] = np.hypot(muon_py, muon_pz)

    library = uproot.interpretation.library._regularize_library("ak")

    expression_context = [(key, key) for key in arrays]
    assert library.group(arrays, expression_context, how=dict) == arrays
