from abc import ABC, abstractmethod
from typing import Any, Dict, List


class DataImportPlugin(ABC):
    """
    This Abstract Base Class is the base class for all data import classes.
    """

    config: Dict[str, Any]
    dataset_name: str
    dataset_eventtype: str

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

    def add_dataset_info(self, dataset_name: str, dataset_eventtype: str) -> None:
        self.dataset_name = dataset_name
        self.dataset_eventtype = dataset_eventtype
