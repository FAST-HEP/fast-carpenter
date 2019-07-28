import pytest
import fast_carpenter.define.variables as fast_vars


@pytest.fixture
def variable_simple():
    define = "sqrt(MET_px**2 + MET_py**2)"
    return define


@pytest.fixture
def variable_jagged():
    define = dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)")
    return define


@pytest.fixture
def variable_jagged_reduce():
    define = dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)", reduce="max")
    return define


@pytest.fixture
def variable_jagged_mask():
    define = dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)",
                  mask="Electron_Pz>0")
    return define


@pytest.fixture
def variable_jagged_mask_reduce():
    define = dict(formula="sqrt(Electron_Px**2 + Electron_Py**2)",
                  mask="Electron_Pz>0",
                  reduce="max")
    return define


def build(name, config):
    builder = fast_vars._build_one_calc("test_" + name, name, config, approach="jagged")
    builder = builder._asdict()
    builder.pop("name")
    return builder


def test_calculation(variable_simple, variable_jagged, variable_jagged_reduce,
                     variable_jagged_mask, variable_jagged_mask_reduce, infile):
    simple = fast_vars.full_evaluate(infile, **build("simple", variable_simple))
    jagged = fast_vars.full_evaluate(infile, **build("jagged", variable_jagged))
    jagged_reduce = fast_vars.full_evaluate(infile, **build("jagged_reduce", variable_jagged_reduce))
    jagged_mask = fast_vars.full_evaluate(infile, **build("jagged_mask", variable_jagged_mask))
    jagged_mask_reduce = fast_vars.full_evaluate(infile, **build("jagged_mask_reduce", variable_jagged_mask_reduce))

    assert len(simple) == len(infile)
    assert len(jagged) == len(infile)
    assert len(jagged_reduce) == len(infile)
    assert len(jagged_mask) == len(infile)
    assert len(jagged_mask_reduce) == len(infile)
    assert all(jagged_mask_reduce <= jagged_reduce)
