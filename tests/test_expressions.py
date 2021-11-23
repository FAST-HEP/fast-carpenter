import pytest
import numpy as np
from awkward0 import JaggedArray
from fast_carpenter import expressions


def test_get_branches(infile):
    valid = infile.allkeys()

    cut = "NMuon > 1"
    branches = expressions.get_branches(cut, valid)
    assert branches == ["NMuon"]

    cut = "NMuon_not_found > 1 and NElectron > 3"
    branches = expressions.get_branches(cut, valid)
    assert branches == ["NElectron"]


def test_evaluate(wrapped_tree):
    Muon_py, Muon_pz = wrapped_tree.arrays(["Muon_Py", "Muon_Pz"], outputtype=tuple)
    mu_pt = expressions.evaluate(wrapped_tree, "sqrt(Muon_Px**2 + Muon_Py**2)")
    assert len(mu_pt) == 100
    assert all(mu_pt.counts == Muon_py.counts)


def test_evaulate_matches_array(wrapped_tree):
    mu_px_array = wrapped_tree.array("Muon_Px") < 0.3
    mu_px_evalu = expressions.evaluate(wrapped_tree, "Muon_Px < 0.3")
    assert (mu_px_evalu == mu_px_array).all().all()


def test_evaluate_bool(full_wrapped_tree):
    all_true = expressions.evaluate(full_wrapped_tree, "Muon_Px == Muon_Px")
    assert all(all_true.all())

    mu_cut = expressions.evaluate(full_wrapped_tree, "NMuon > 1")
    ele_cut = expressions.evaluate(full_wrapped_tree, "NElectron > 1")
    jet_cut = expressions.evaluate(full_wrapped_tree, "NJet > 1")
    mu_px = expressions.evaluate(full_wrapped_tree, "Muon_Px > 0.3")
    mu_px = mu_px.pad(2)[:, 1]
    combined = mu_cut & (ele_cut | jet_cut) & mu_px
    assert np.count_nonzero(combined) == 2


def test_evaluate_dot(wrapped_tree):
    wrapped_tree.new_variable("Muon.Px", wrapped_tree.array("Muon_Px"))
    all_true = expressions.evaluate(wrapped_tree, "Muon.Px == Muon_Px")
    assert all(all_true.all())


def test_constants(infile):
    nan_1_or_fewer_mu = expressions.evaluate(infile, "where(NMuon > 1, NMuon, nan)")
    assert np.count_nonzero(~np.isnan(nan_1_or_fewer_mu)) == 289

    ninf_1_or_fewer_mu = expressions.evaluate(infile, "where(NMuon > 1, NMuon, -inf)")
    assert np.count_nonzero(np.isfinite(ninf_1_or_fewer_mu)) == 289


def test_3D_jagged(wrapped_tree):
    fake_3d = [[np.arange(i + 1) + j
                for i in range(j % 3)]
               for j in range(len(wrapped_tree))]
    fake_3d = JaggedArray.fromiter(fake_3d)
    wrapped_tree.new_variable("Fake3D", fake_3d)
    assert isinstance(fake_3d.count(), JaggedArray)
    assert all((fake_3d.copy().count() == fake_3d.count()).all())

    aliased = expressions.evaluate(wrapped_tree, "Fake3D")
    assert (aliased == fake_3d).all().all().all()

    doubled = expressions.evaluate(wrapped_tree, "Fake3D * 2")
    assert (doubled == fake_3d * 2).all().all().all()
    assert len(doubled[0, :, :]) == 0
    assert doubled[1, 0, :] == [2]
    assert doubled[2, 0, :] == [4]
    assert all(doubled[2, 1, :] == [4, 6])

    doubled = expressions.evaluate(wrapped_tree, "Fake3D + Fake3D")
    assert (doubled == fake_3d * 2).all().all().all()
    assert len(doubled[0, :, :]) == 0
    assert doubled[1, 0, :] == [2]
    assert doubled[2, 0, :] == [4]
    assert all(doubled[2, 1, :] == [4, 6])

    fake_3d_2 = [[np.arange(i + 3) + j
                  for i in range(j % 2)]
                 for j in range(len(wrapped_tree))]
    fake_3d_2 = JaggedArray.fromiter(fake_3d_2)
    wrapped_tree.new_variable("SecondFake3D", fake_3d_2)

    with pytest.raises(ValueError) as e:
        expressions.evaluate(wrapped_tree, "SecondFake3D + Fake3D")
    assert "Cannot broadcast" in str(e)


@pytest.mark.parametrize('input, expected', [
    ("Muon.Px > 30", ("Muon__DOT__Px > 30", {'Muon__DOT__Px': 'Muon.Px'})),
    ("events.Muon.Px > 30", ("events__DOT__Muon__DOT__Px > 30",
                             {'events__DOT__Muon__DOT__Px': 'events.Muon.Px'})),
    ('l1CaloTowerTree.L1CaloTowerTree.L1CaloTower.et > 50',
        ('l1CaloTowerTree__DOT__L1CaloTowerTree__DOT__L1CaloTower__DOT__et > 50',
         {'l1CaloTowerTree__DOT__L1CaloTowerTree__DOT__L1CaloTower__DOT__et':
             'l1CaloTowerTree.L1CaloTowerTree.L1CaloTower.et'}))
])
def test_preprocess_expression(input, expected):
    # note: maybe hypothesis.strategies.from_regex is better than parametrize
    clean_expr, alias_dict = expressions.preprocess_expression(input)
    assert clean_expr == expected[0]
    assert alias_dict == expected[1]


def test_broadcast(wrapped_tree):
    expressions.evaluate(wrapped_tree, "NJet * Jet_Py + NElectron * Jet_Px")

    with pytest.raises(ValueError):
        expressions.evaluate(wrapped_tree, "Jet_Py + Muon_Px")
