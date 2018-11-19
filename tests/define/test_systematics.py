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
