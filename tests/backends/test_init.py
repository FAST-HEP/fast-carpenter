import pytest
import fast_carpenter.backends as backends


def test_get_backend():
    coffea_back = backends.get_backend("coffea:dask")
    assert hasattr(coffea_back, "execute")

    with pytest.raises(ValueError) as e:
        backends.get_backend("doesn't exist")
    assert "Unknown backend" in str(e)
