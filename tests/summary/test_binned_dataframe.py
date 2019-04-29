import pytest
import fast_carpenter.summary.binned_dataframe as bdf
from . import dummy_binning_descriptions as binning
from ..conftest import FakeBEEvent


@pytest.fixture
def config_1():
    return dict(binning=[binning.bins_met_px,
                         binning.bins_py],
                weights=binning.weight_list)


@pytest.fixture
def config_2():
    return dict(binning=[binning.bins_py,
                         binning.bins_met_px,
                         binning.bins_nmuon],
                weights=binning.weight_dict)


@pytest.fixture
def binned_df_1(tmpdir, config_1):
    return bdf.BinnedDataframe("binned_df_1", out_dir="somewhere", **config_1)


@pytest.fixture
def binned_df_1_copied(tmpdir, config_1):
    return bdf.BinnedDataframe("binned_df_1", out_dir="somewhere", **config_1)


def test_BinnedDataframe(binned_df_1, tmpdir):
    assert binned_df_1.name == "binned_df_1"
    assert len(binned_df_1._binnings) == 2
    # bin length for met_px: nbins, plus 1 for edge, plus 2 for +-inf
    assert binned_df_1._bin_dims[0] == "MET_px"
    assert len(binned_df_1._binnings[0]) == 10 + 1 + 2
    assert len(binned_df_1._weights) == 1


@pytest.fixture
def binned_df_2(tmpdir, config_2):
    return bdf.BinnedDataframe("binned_df_2", out_dir="somewhere", **config_2)


def test_BinnedDataframe_run_mc(binned_df_1, tmpdir, infile):
    chunk = FakeBEEvent(infile, "mc")
    collector = binned_df_1.collector()

    binned_df_1.event(chunk)
    dataset_readers_list = (("test_dataset", (binned_df_1,)),)
    results = collector._prepare_output(dataset_readers_list)

    totals = results.sum()
    # Based on: events->Draw("Jet_Py", "", "goff")
    assert totals["n"] == 4616

    # Based on:
    # events->Draw("EventWeight * (Jet_Py/Jet_Py)>>htemp", "", "goff")
    # htemp->GetMean() * htemp->GetEntries()
    assert totals["EventWeight:sumw"] == pytest.approx(231.91339)


def test_BinnedDataframe_run_data(binned_df_2, tmpdir, infile):
    chunk = FakeBEEvent(infile, "data")
    binned_df_2.event(chunk)


def test_BinnedDataframe_run_twice(binned_df_1, tmpdir, infile):
    chunk = FakeBEEvent(infile, "mc")
    collector = binned_df_1.collector()

    binned_df_1.event(chunk)
    binned_df_1.event(chunk)

    dataset_readers_list = (("test_dataset", (binned_df_1,)),)
    results = collector._prepare_output(dataset_readers_list)

    totals = results.sum()
    # Based on: events->Draw("Jet_Py", "", "goff")
    assert totals["n"] == 4616 * 2

    # Based on:
    # events->Draw("EventWeight * (Jet_Py/Jet_Py)>>htemp", "", "goff")
    # htemp->GetMean() * htemp->GetEntries()
    assert totals["EventWeight:sumw"] == pytest.approx(231.91339 * 2)


@pytest.fixture
def run_twice_data_mc(binned_df_1, binned_df_1_copied, infile):
    chunk_mc = FakeBEEvent(infile, "mc")
    chunk_data = FakeBEEvent(infile, "data")

    binned_df_1.event(chunk_mc)
    binned_df_1.event(chunk_mc)
    binned_df_1_copied.event(chunk_data)
    binned_df_1_copied.event(chunk_data)

    return binned_df_1, (("test_mc", (binned_df_1,)), ("test_data", (binned_df_1_copied,)),)


@pytest.mark.parametrize(["dataset_col", "pad_missing"], [(True, True), (True, False), (False, False), (False, True)])
def test_binneddataframe_run_twice_data_mc(run_twice_data_mc, dataset_col, pad_missing):
    binned_df_1, dataset_readers_list = run_twice_data_mc
    binned_df_1._pad_missing = pad_missing
    binned_df_1._dataset_col = dataset_col
    collector = binned_df_1.collector()
    results = collector._prepare_output(dataset_readers_list)

    assert results.index.nlevels == 2 + int(dataset_col)
    if pad_missing or not dataset_col:
        length = (4 * 12) * (1 + int(dataset_col))
    else:
        length = 95  # When dataset_col True and pad_missing False one bin is missing
        assert len(results) == length

    totals = results.sum()
    # Based on: events->Draw("Jet_Py", "", "goff")
    assert totals["n"] == 4616 * 4

    # Based on:
    # events->Draw("EventWeight * (Jet_Py/Jet_Py)>>htemp", "", "goff")
    # htemp->GetMean() * htemp->GetEntries()
    assert totals["EventWeight:sumw"] == pytest.approx(231.91339 * 2)
