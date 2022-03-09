import awkward as ak


def check_data(data, n_data, n_nonzero, n_mask):
    assert len(data) == n_data
    assert data.count_nonzero() == n_nonzero
    if data.tree._mask is not None:
        assert len(data.tree._mask) == n_mask


def test_single_mask(fake_data_events, full_wrapped_tree, at_least_two_muons, tmpdir):
    full_size = len(fake_data_events)
    check_data(fake_data_events, n_data=full_size, n_nonzero=full_size, n_mask=full_size)
    at_least_two_muons.event(fake_data_events)
    check_data(fake_data_events, n_data=full_size, n_nonzero=289, n_mask=full_size)

    mask = fake_data_events.tree._mask
    assert ak.count_nonzero(mask) == 289
    assert ak.all(fake_data_events.tree["Muon_Pz"] == ak.mask(full_wrapped_tree["Muon_Pz"], mask))


def test_single_mask_twice(fake_data_events, full_wrapped_tree, at_least_two_muons, tmpdir):
    full_size = len(fake_data_events)
    check_data(fake_data_events, n_data=full_size, n_nonzero=full_size, n_mask=full_size)
    # applying the same cut twice should not change the result
    at_least_two_muons.event(fake_data_events)
    at_least_two_muons.event(fake_data_events)
    check_data(fake_data_events, n_data=full_size, n_nonzero=289, n_mask=full_size)

    mask = fake_data_events.tree._mask
    assert ak.count_nonzero(mask) == 289
    assert ak.all(fake_data_events.tree["Muon_Pz"] == ak.mask(full_wrapped_tree["Muon_Pz"], mask))


def test_complex_mask(fake_data_events, full_wrapped_tree, at_least_two_muons_plus, tmpdir):
    full_size = len(fake_data_events)
    check_data(fake_data_events, n_data=full_size, n_nonzero=full_size, n_mask=full_size)

    at_least_two_muons_plus.event(fake_data_events)

    check_data(fake_data_events, n_data=full_size, n_nonzero=2, n_mask=full_size)

    mask = fake_data_events.tree._mask
    assert ak.count_nonzero(mask) == 2
    assert ak.all(fake_data_events.tree["Muon_Pz"] == ak.mask(full_wrapped_tree["Muon_Pz"], mask))


def test_multi_mask(fake_data_events, full_wrapped_tree, at_least_two_muons, at_least_two_muons_plus, tmpdir):
    full_size = len(fake_data_events)
    check_data(fake_data_events, n_data=full_size, n_nonzero=full_size, n_mask=full_size)

    at_least_two_muons.event(fake_data_events)
    at_least_two_muons_plus.event(fake_data_events)

    check_data(fake_data_events, n_data=full_size, n_nonzero=2, n_mask=full_size)

    mask = fake_data_events.tree._mask
    assert ak.count_nonzero(mask) == 2
    assert ak.all(fake_data_events.tree["Muon_Pz"] == ak.mask(full_wrapped_tree["Muon_Pz"], mask))
