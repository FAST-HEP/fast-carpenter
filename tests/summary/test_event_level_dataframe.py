import fast_carpenter.summary.event_level_dataframe as edf


def test_EventByEventDataframe(tmpdir):
    # TODO: Make this a proper set of tests
    assert hasattr(edf, "Collector")
