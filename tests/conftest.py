import pytest
import uproot3
import uproot as uproot4

import fast_carpenter.selection.stage as stage
from fast_carpenter.testing import FakeBEEvent, FakeEventRange


@pytest.fixture
def test_input_file():
    return "tests/data/CMS_HEP_tutorial_ww.root"


@pytest.fixture
def uproot3_tree(test_input_file):
    return uproot3.open(test_input_file)["events"]


@pytest.fixture
def uproot4_tree(test_input_file):
    return uproot4.open(test_input_file)["events"]


@pytest.fixture
def size_of_test_sample(uproot4_tree):
    return uproot4_tree.num_entries


@pytest.fixture
def event_range():
    return FakeEventRange(100, 200, 100)


@pytest.fixture
def full_event_range(size_of_test_sample):
    return FakeEventRange(0, size_of_test_sample, 0)


def wrap_uproot4_tree(input_tree, event_range):
    from fast_carpenter import tree_adapter
    tree = tree_adapter.create_ranged(
        {
            "adapter": "uproot4", "tree": input_tree,
            "start": event_range.start_entry, "stop": event_range.stop_entry,
        }
    )
    return tree


@pytest.fixture
def wrapped_uproot4_tree(uproot4_tree, event_range):
    return wrap_uproot4_tree(uproot4_tree, event_range)


@pytest.fixture
def full_wrapped_uproot4_tree(uproot4_tree, full_event_range):
    return wrap_uproot4_tree(uproot4_tree, full_event_range)


@pytest.fixture
def full_wrapped_masked_uproot4_tree(uproot4_tree, full_event_range):
    from fast_carpenter import tree_adapter
    return tree_adapter.create_masked(
        {
            "adapter": "uproot4", "tree": uproot4_tree,
            "start": full_event_range.start_entry, "stop": full_event_range.stop_entry,
        })


@pytest.fixture
def at_least_two_muons(tmpdir):
    return stage.CutFlow("cut_at_least_one_muon", str(tmpdir), selection="NMuon > 1", weights="EventWeight")


@pytest.fixture
def at_least_two_muons_plus(tmpdir):
    return stage.CutFlow(
        "cutflow_2",
        str(tmpdir),
        selection={
            "All": [
                "NMuon > 1",
                {"Any": ["NElectron > 1", "NJet > 1"]},
                {"reduce": 1, "formula": "Muon_Px > 0.3"}
            ]
        },
        weights="EventWeight"
    )


@pytest.fixture
def fake_data_events(full_wrapped_masked_uproot4_tree):
    return FakeBEEvent(full_wrapped_masked_uproot4_tree, "data")


@pytest.fixture
def fake_sim_events(full_wrapped_masked_uproot4_tree):
    return FakeBEEvent(full_wrapped_masked_uproot4_tree, "mc")


# setting the default to uproot4
input_tree = uproot4_tree
wrapped_tree = wrapped_uproot4_tree
full_wrapped_tree = full_wrapped_uproot4_tree
masked_tree = full_wrapped_masked_uproot4_tree
