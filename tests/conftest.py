import pytest

from fasthep_carpenter.data_mapping import create_mapping
from fasthep_carpenter.data_import import get_data_import_plugin
from fasthep_carpenter.protocols import EventRange


@pytest.fixture
def test_input_file():
    return "tests/data/CMS_HEP_tutorial_ww.root"


@pytest.fixture
def test_multi_tree_input_file():
    return "tests/data/2kmu.root"


@pytest.fixture
def event_range():
    return EventRange(
        start=100,
        stop=200,
        block_size=100,
    )


@pytest.fixture
def full_event_range():
    return EventRange(
        start=0,
        stop=-1,
        block_size=0,
    )


@pytest.fixture(params=["uproot4"])
def data_mapping_with_tree(request, test_input_file):
    uproot_version = request.param
    data_import_plugin = get_data_import_plugin(uproot_version)
    f = data_import_plugin.open([test_input_file])

    mapping = create_mapping(
        input_file=f,
        treenames=["events"],
        methods=uproot_version,
        connector="tree",
    )
    mapping.add_dataset_info("tree_test", "mc")
    return mapping


@pytest.fixture(params=["uproot4"])
def data_mapping_with_file(request, test_input_file):
    uproot_version = request.param
    data_import_plugin = get_data_import_plugin(uproot_version)
    f = data_import_plugin.open([test_input_file])

    mapping = create_mapping(
        input_file=f,
        treenames=["events"],
        methods=uproot_version,
        connector="file",
    )
    mapping.add_dataset_info("file_test", "mc")
    return mapping


@pytest.fixture(params=["uproot4"])
def data_mapping_with_multi_tree(request, test_multi_tree_input_file):
    uproot_version = request.param
    data_import_plugin = get_data_import_plugin(uproot_version)
    f = data_import_plugin.open([test_multi_tree_input_file])

    mapping = create_mapping(
        input_file=f,
        treenames=["l1CaloTowerEmuTree/L1CaloTowerTree", "l1EventTree/L1EventTree"],
        methods=uproot_version,
        connector="file",
    )
    mapping.add_dataset_info("multi_tree_test", "mc")
    return mapping
