import pytest

from fasthep_carpenter.data_import import get_data_import_plugin, register_data_import_plugin, unregister_data_import_plugin
from fasthep_carpenter.data_import._uproot4 import Uproot4DataImport
from fasthep_carpenter.protocols import DataImportPlugin


def test_register_data_import_plugin():
    class TestPlugin(DataImportPlugin):

        def open(self, input_files):
            pass

    register_data_import_plugin("test_plugin", TestPlugin)
    assert isinstance(get_data_import_plugin("test_plugin"), TestPlugin)
    unregister_data_import_plugin("test_plugin")


def test_get_data_import_plugin():
    assert isinstance(get_data_import_plugin("uproot4"), DataImportPlugin)
    assert isinstance(get_data_import_plugin("uproot4"), Uproot4DataImport)


def test_get_data_import_plugin_error():
    with pytest.raises(ValueError):
        get_data_import_plugin("test_plugin")
