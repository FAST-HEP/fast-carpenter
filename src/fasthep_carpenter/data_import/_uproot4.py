from typing import Any, Dict, List

from ..protocols import DataImportPlugin


class Uproot4DataImport(DataImportPlugin):
    """
    This class is a wrapper around the uproot library.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

    def open(self, paths: List[str]) -> Any:
        """
        This method is called by the importer to open the file.
        """
        import uproot

        if len(paths) != 1:
            # TODO - support multiple paths
            raise AttributeError("Multiple paths not yet supported")
        # Try to open the tree - some machines have configured limitations
        # which prevent memmaps from begin created. Use a fallback - the
        # localsource option
        input_file = paths[0]
        try:
            rootfile = uproot.open(input_file)
        except MemoryError:
            rootfile = uproot.open(input_file, file_handler=uproot.source.chunk.Source)
        return rootfile
