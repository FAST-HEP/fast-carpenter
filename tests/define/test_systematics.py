import pytest
import numpy as np
import fast_carpenter.define.systematics as fast_syst
from ..conftest import FakeBEEvent, Namespace


@pytest.fixture
def weights_1():
    return dict(no_variation={"nominal": "NoVariation"},
                varied={"nominal": "Varied1", "up": "Varied1Up", "down": "Varied1__DOWN"})


@pytest.fixture
def systematic_variations_1(weights_1):
    return fast_syst.SystematicWeights("systematic_variations_1", "somewhere", weights_1)


class FakeTree(Namespace):
    def array(self, attr, **kwargs):
        return getattr(self, attr)

    def new_variable(self, name, variable):
        setattr(self, name, variable)


@pytest.fixture
def fake_file():
    tree = FakeTree(Varied1=np.arange(3),
                    Varied1Up=np.arange(3) * 1.5,
                    Varied1__DOWN=np.arange(3) * 0.75,
                    NoVariation=np.full(3, 4),
                    )

    return tree


def test_systematic_variations_1_mc(systematic_variations_1, fake_file):
    chunk = FakeBEEvent(fake_file, "mc")
    systematic_variations_1.event(chunk)

    assert all(chunk.tree.weight_nominal == [0, 4, 8])
    assert all(chunk.tree.weight_varied_up == [0, 6., 12.])
    assert all(chunk.tree.weight_varied_down == [0, 3., 6.])


def test_normalize_one_variation():
    cfg = dict(nominal="one", up="two")
    stage_name = "test_normalize_one_variation"
    out = fast_syst._normalize_one_variation(stage_name, cfg, "test1", tuple())
    assert len(out) == 2
    assert out["nominal"] == "one"
    assert out["up"] == "two"

    with pytest.raises(fast_syst.BadSystematicWeightsConfig) as e:
        fast_syst._normalize_one_variation(stage_name, dict(a="bad"), "test2", tuple())
    assert "nominal" in str(e)

    with pytest.raises(fast_syst.BadSystematicWeightsConfig) as e:
        fast_syst._normalize_one_variation(stage_name, dict(nominal="one", a="bad"), "test3", tuple())
    assert "unknown key" in str(e)

    out = fast_syst._normalize_one_variation(stage_name, dict(nominal="one", a="bad"), "test3", tuple("a"))
    assert len(out) == 2
    assert out["nominal"] == "one"
    assert out["a"] == "bad"

    out = fast_syst._normalize_one_variation(stage_name, "just_a_string", "test4", tuple())
    assert len(out) == 1
    assert out["nominal"] == "just_a_string"


def test_build_variations():
    pileup = dict(nominal="PILEUP", up="PILEUP_UP", down="PILEUP_DOWN")
    isolation = dict(nominal="Iso", up="IsoUp")
    another = dict(nominal="Blahblah", left="BlahblahLeft")

    all_vars = dict(pileup=pileup, isolation=isolation, another=another)
    stage_name = "test_build_variations"
    formulae = fast_syst._build_variations(stage_name, all_vars, out_fmt="Weight_{}_test")
    assert len(formulae) == 5
    assert isinstance(formulae, list)
    assert all(map(lambda x: isinstance(x, dict) and len(x) == 1, formulae))
    formulae = {list(i.keys())[0]: list(i.values())[0] for i in formulae}
    assert formulae["Weight_nominal_test"] == "(PILEUP)*(Iso)*(Blahblah)"
    assert formulae["Weight_pileup_up_test"] == "(PILEUP_UP)*(Iso)*(Blahblah)"
    assert formulae["Weight_pileup_down_test"] == "(PILEUP_DOWN)*(Iso)*(Blahblah)"
    assert formulae["Weight_isolation_up_test"] == "(PILEUP)*(IsoUp)*(Blahblah)"
    assert formulae["Weight_another_left_test"] == "(PILEUP)*(Iso)*(BlahblahLeft)"
