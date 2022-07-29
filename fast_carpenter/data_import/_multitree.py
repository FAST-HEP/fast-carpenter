from ..data_mapping import create_mapping
from ._base import DataImportPlugin


class MultiTreeImport(DataImportPlugin):
    def __init__(self, config):
        super().__init__(config)
        self._process_config()

    def _process_config(self):
        pass

    def open(self, paths):
        from collections.abc import Sequence

        import uproot

        # uproot.XRootDSource.defaults["parallel"] = False

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

        trees = []
        if "treename" in self.config:
            if isinstance(self.config["treename"], str):
                trees.append(self.config["treename"])
            elif isinstance(self.config["treename"], Sequence):
                trees.extend(self.config["treename"])
            else:
                raise AttributeError("treename must be a string or list of strings")
                # trees = self.config["treename"]

        data_mapping = create_mapping(
            input_file=rootfile,
            treenames=trees,
            connector="file",
            methods="uproot4",
        )

        return data_mapping
