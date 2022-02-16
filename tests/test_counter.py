import pytest
from fast_carpenter.selection.filters import Counter


@pytest.fixture
def weight_names():
    return [
        "EventWeight",
        # "MuonWeight", "ElectronWeight", "JetWeight",
    ]


@pytest.fixture
def counter(weight_names):
    return Counter(weight_names)


def test_init(weight_names, full_wrapped_tree):
    c = Counter(weight_names)
    assert c._weights == weight_names
    assert c.counts == (0, 0.0)
    assert c._w_counts == (0.0)


def test_increment_mc(counter, full_wrapped_tree):
    counter.increment(full_wrapped_tree, is_mc=True)
    n_events = len(full_wrapped_tree)
    expected_weighted_sum = 229.94895935058594

    assert counter._w_counts == (expected_weighted_sum)
    assert counter.counts == (n_events, expected_weighted_sum)


def test_increment_data(counter, full_wrapped_tree):
    counter.increment(full_wrapped_tree, is_mc=False)
    n_events = len(full_wrapped_tree)
    assert counter._w_counts == (n_events)
    assert counter.counts == (n_events, n_events)


def test_add(counter, full_wrapped_tree):
    counter.increment(full_wrapped_tree, is_mc=True)
    counter.add(counter)

    n_events = len(full_wrapped_tree)
    expected_weighted_sum = 229.94895935058594

    assert counter._w_counts == (expected_weighted_sum * 2,)
    assert counter.counts == (n_events * 2, expected_weighted_sum * 2)

def test_increment_without_weights(full_wrapped_tree):
    counter = Counter([])
    counter.increment(full_wrapped_tree, is_mc=True)
    n_events = len(full_wrapped_tree)

    assert counter._w_counts[0] == n_events
    assert counter.counts == (n_events, n_events)
