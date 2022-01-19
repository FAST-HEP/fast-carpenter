import awkward as ak
import pytest

import fast_carpenter.define.variables as fast_vars


def build(name, config):
    builder = fast_vars._build_one_calc("test_" + name, name, config, approach="jagged")
    builder = builder._asdict()
    builder.pop("name")
    return builder


@pytest.mark.parametrize("name, define, mask", [
    ("simple", dict(formula="sqrt(MET_px**2 + MET_py**2)"), None),
    ("jagged", dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)"), None),
    ("jagged_reduce", dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)", reduce="max"), None),
    ("jagged_mask", dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)"), "Electron_Pz>0"),
    ("jagged_mask_reduce", dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)", reduce="max"), "Electron_Pz>0"),
])
def test_calculation(name, define, mask, wrapped_tree):
    result = fast_vars.full_evaluate(wrapped_tree, **build(name, define))

    assert ak.count_nonzero(result) > 0
    assert len(result) == len(wrapped_tree)
    if mask is None or "reduce" not in define:
        return

    define["mask"] = mask
    result_masked = fast_vars.full_evaluate(wrapped_tree, **build(name, define))

    assert len(result_masked) == len(wrapped_tree)
    assert ak.all(result_masked <= result)
