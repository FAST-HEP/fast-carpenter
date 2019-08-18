import pytest
import numpy as np


def test_add_retrieve(wrapped_tree):
    muon_px = wrapped_tree.array("Muon_Px")
    assert len(muon_px) == 100

    muon_py, muon_pz = wrapped_tree.arrays(["Muon_Py", "Muon_Pz"], outputtype=tuple)
    muon_momentum = np.hypot(muon_py, muon_pz)
    wrapped_tree.new_variable("Muon_momentum", muon_momentum)
    retrieve_momentum = wrapped_tree.array("Muon_momentum")
    assert (retrieve_momentum == muon_momentum).flatten().all()


def test_overwrite(wrapped_tree):
    muon_px = wrapped_tree.array("Muon_Px")
    with pytest.raises(ValueError) as err:
        wrapped_tree.new_variable("Muon_Px", muon_px / muon_px)
    assert "Muon_Px" in str(err)
    assert len(wrapped_tree.keys(filtername=lambda x: x.decode() == "Muon_Px")) == 1
