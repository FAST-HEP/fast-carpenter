from typing import Any, Dict, List

from ._base import DataImportBase


class Uproot3DataImport(DataImportBase):
    """
    This class is a wrapper around the uproot3 library.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

    def _process_config(self):
        pass

    def open(self, paths: List[str]) -> Any:
        """
        This method is called by the importer to open the file.
        """
        import uproot3
        if len(paths) != 1:
            # TODO - support multiple paths
            raise AttributeError("Multiple paths not yet supported")
        # Try to open the tree - some machines have configured limitations
        # which prevent memmaps from begin created. Use a fallback - the
        # localsource option
        input_file = paths[0]
        try:
            rootfile = uproot3.open(input_file)
        except MemoryError:
            rootfile = uproot3.open(input_file, localsource=uproot3.FileSource.defaults)
        return rootfile
