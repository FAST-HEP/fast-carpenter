import pytest

import awkward as ak

from fast_carpenter.weights import get_unweighted_increment, get_weighted_increment, extract_weights


@pytest.fixture
def event_weights():
    weights = ak.Array(
        [
            [1, 2, 3],  # event level weights
            [2, 5, 7],  # event level weights
            # [[0.4], [0.5, 0.6], [0.7, 0.8, 0.9]],  # object, e.g. muon, level weights
        ]
    )
    # TODO: to support awkward arrays as weights, simply remove the "to_numpy"
    return weights.to_numpy()


@pytest.fixture
def event_mask():
    return ak.Array([True, False, True])


def test_unweighted_increment(event_weights):
    inc = get_unweighted_increment(event_weights, None)
    assert inc == len(event_weights[0])


def test_weighted_increment(event_weights):
    inc = get_weighted_increment(event_weights, None)
    assert ak.all(inc == [6, 14])


def test_extract_weights(full_wrapped_tree):
    weights = extract_weights(full_wrapped_tree, ["EventWeight"])
    assert len(weights) == 1
    assert len(weights[0]) == len(full_wrapped_tree)


def test_extract_weights(fake_data_events):
    weights = extract_weights(fake_data_events, ["EventWeight"])
    assert len(weights) == 1
    assert len(weights[0]) == len(fake_data_events)


def test_extract_weights_no_weights(fake_data_events):
    weights = extract_weights(fake_data_events, [])
    assert len(weights) == 0