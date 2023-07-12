from pathlib import Path
from typing import Optional

from ..protocols import DataImportPlugin
from ._multitree import MultiTreeImport
from ._uproot4 import Uproot4DataImport

_DATA_IMPORT_PLUGINS = {
    "uproot4": Uproot4DataImport,
    "multitree": MultiTreeImport,
}


def register_data_import_plugin(
    plugin_name: str, plugin_class: DataImportPlugin
) -> None:
    """
    Register a new plugin_name for data import.
    """
    _DATA_IMPORT_PLUGINS[plugin_name] = plugin_class

def unregister_data_import_plugin(plugin_name: str) -> None:
    """
    Unregister a plugin_name for data import.
    """
    if plugin_name in _DATA_IMPORT_PLUGINS:
        _DATA_IMPORT_PLUGINS.pop(plugin_name)

def _process_plugin_config(
    plugin_name: str, plugin_config: Optional[str] = None
) -> None:
    """
    Process the plugin configuration file.
    Reads the "register" and "plugin_name" sections to register and configure the plugin.
    """
    if plugin_config is None:
        return
    plugin_config = Path(plugin_config)
    if not plugin_config.exists():
        raise ValueError(f"Plugin config file {plugin_config} does not exist")
    if not plugin_config.is_file():
        raise ValueError(f"Plugin config file {plugin_config} is not a file")
    
    import fast_curator

    return fast_curator.read.from_yaml(plugin_config)


def get_data_import_plugin(
    plugin_name: str, plugin_config: Optional[Path] = None
) -> DataImportPlugin:
    """
    Get a data import plugin by name.
    """
    config = _process_plugin_config(plugin_name, plugin_config)
    if plugin_name not in _DATA_IMPORT_PLUGINS:
        raise ValueError(f"Unknown data import plugin: {plugin_name}")
    return _DATA_IMPORT_PLUGINS[plugin_name](config)
