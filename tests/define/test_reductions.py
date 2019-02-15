import pytest
import numpy as np
from awkward import JaggedArray
import fast_carpenter.define.reductions as reductions


@pytest.fixture
def jagged_1():
    boundaries = [0, 3, 5, 6, 9, 12, 12]
    return JaggedArray(boundaries[:-1], boundaries[1:], [0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0, 11.0])


def test_jagged_nth(jagged_1):
    get_first_second = reductions.JaggedNth(1, np.nan)
    reduced = get_first_second(jagged_1)
    assert reduced[0] == 1.1
    assert reduced[1] == 4.4
    assert np.isnan(reduced[2])
    assert reduced[3] == 7.7
    assert reduced[4] == 10.0
    assert np.isnan(reduced[5])


def test_jagged_nth_default_int(jagged_1):
    get_first_second = reductions.JaggedNth(1, 0, force_float=False)
    reduced = get_first_second(jagged_1)
    assert reduced[0] == 1
    assert reduced[1] == 4
    assert reduced[2] == 0
    assert reduced[3] == 7
    assert reduced[4] == 10
    assert reduced[5] == 0


def test_jagged_nth_default_float(jagged_1):
    get_first_second = reductions.JaggedNth(1, 0)
    reduced = get_first_second(jagged_1)
    assert reduced[0] == 1.1
    assert reduced[1] == 4.4
    assert reduced[2] == 0.
    assert reduced[3] == 7.7
    assert reduced[4] == 10.0
    assert reduced[5] == 0.0


def test_jagged_counts(jagged_1):
    get_counts = reductions.JaggedProperty("counts")
    reduced = get_counts(jagged_1)
    assert reduced[0] == 3
    assert reduced[1] == 2
    assert reduced[2] == 1
    assert reduced[3] == 3
    assert reduced[4] == 3
    assert reduced[5] == 0


def test_jagged_sum(jagged_1):
    get_sum = reductions.JaggedMethod("sum")
    reduced = get_sum(jagged_1)
    assert reduced[0] == pytest.approx(3.3)
    assert reduced[1] == pytest.approx(7.7)
    assert reduced[2] == pytest.approx(5.5)
    assert reduced[3] == pytest.approx(23.1)
    assert reduced[4] == pytest.approx(30.9)
    assert reduced[5] == pytest.approx(0)


def test_get_awkward_reduction():
    fourth_element = reductions.get_awkward_reduction("test_get_awkward_reduction", 3, fill_missing=np.nan)
    assert isinstance(fourth_element, reductions.JaggedNth)
    assert fourth_element.index == 3

    sum_elements = reductions.get_awkward_reduction("test_get_awkward_reduction", "sum")
    assert isinstance(sum_elements, reductions.JaggedMethod)
    assert sum_elements.method_name == "sum"

    count_elements = reductions.get_awkward_reduction("test_get_awkward_reduction", "counts")
    assert isinstance(count_elements, reductions.JaggedProperty)
    assert count_elements.prop_name == "counts"

    with pytest.raises(reductions.BadReductionConfig) as e_info:
        reductions.get_awkward_reduction("test_get_awkward_reduction", {"some": "dict"})
    assert "not a string or an int" in str(e_info.value)

    with pytest.raises(reductions.BadReductionConfig) as e_info:
        reductions.get_awkward_reduction("test_get_awkward_reduction", "non_existent_method")
    assert "Unknown method" in str(e_info.value)
