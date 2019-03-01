import numpy as np


def test_add_retrieve(wrapped_tree):
    Muon_px = wrapped_tree.array("Muon_Px")
    assert len(Muon_px) == 100

    Muon_py, Muon_pz = wrapped_tree.arrays(["Muon_Py", "Muon_Pz"], outputtype=tuple)
    Muon_momentum = np.hypot(Muon_py, Muon_pz)
    wrapped_tree.new_variable("Muon_momentum", Muon_momentum)
    retrieve_momentum = wrapped_tree.array("Muon_momentum")
    assert (retrieve_momentum == Muon_momentum).flatten().all()
