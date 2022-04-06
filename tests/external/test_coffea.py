import awkward as ak
from fast_carpenter.backends.coffea import CoffeaConnector


class DummyCoffeaDataset(object):
    metadata = {
        "entrystart": 0,
        "entrystop": 100,
        "dataset": "dummy",
    }
    size = 100
    fields = {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9], "d": [10, 11, 12]}

    def __len__(self):
        return self.size

    def __getitem__(self, key):
        return self.fields[key]


def test_connector():
    dataset = DummyCoffeaDataset()
    connector = CoffeaConnector(dataset)

    assert connector.num_entries == dataset.size
    assert connector.dataset == dataset.metadata["dataset"]
    assert connector.start == dataset.metadata["entrystart"]
    assert connector.stop == dataset.metadata["entrystop"]

    arrays = connector.arrays(["a", "b", "c", "d"])
    for key in arrays.keys():
        array = arrays[key]
        field = dataset[key]
        assert ak.all(array == field)
