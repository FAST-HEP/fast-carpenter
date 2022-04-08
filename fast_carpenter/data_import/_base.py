from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DataImportBase(ABC):
    """
    This Abstract Base Class is the base class for all data import classes.
    """
    config: Dict[str, Any]

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    @abstractmethod
    def _process_config(self) -> None:
        """
        Process the configuration.
        """
        pass

    @abstractmethod
    def open(self, paths: List[str]) -> Any:
        """
        This method is called by the importer to open the files.
        """
        pass
