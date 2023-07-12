"""Module containing protocols and Abstract Base Classes (ABCs) for the fasthep-carpenter."""
from abc import ABC, abstractmethod
from collections.abc import MutableMapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Protocol, Tuple
from functools import reduce

import pandas as pd


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
    def open(self, paths: List[str]) -> Any:
        """
        This method is called by the importer to open the files.
        """
        pass

    def add_dataset_info(self, dataset_name: str, dataset_eventtype: str) -> None:
        self.dataset_name = dataset_name
        self.dataset_eventtype = dataset_eventtype


@dataclass
class EventRange:
    start: int = 0
    stop: int = -1
    block_size: int = 0


@dataclass
class FilterDescription:
    """
    A class to hold the filter description.
    A filter has a name and a description.
    The description is a dictionary that contains "All" or "Any" as keys,
    which then point to a list of either strings or dictionaries containing "All" or "Any" as keys.
    """
    name: str
    description: Dict[str, Any]


##################
# For data_mapping
##################
class ArrayMethodsProtocol(Protocol):
    @staticmethod
    def all(data: Any, **kwargs) -> bool:
        raise NotImplementedError()

    @staticmethod
    def arraydict_to_pandas(arraydict: Dict[str, Any]) -> pd.DataFrame:
        """
        Converts a dictionary of arrays to a pandas DataFrame.
        """
        raise NotImplementedError()

    @staticmethod
    def awkward_from_iter(data: Iterable) -> Any:
        raise NotImplementedError()

    @staticmethod
    def extract_array_dict(data: Any, keys: List[str]) -> Dict[str, Any]:
        """
        Returns a dictionary of arrays for the given keys.
        """
        raise NotImplementedError()

    @staticmethod
    def array_exporter(dict_of_arrays: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def array_from_tree(tree: Any, key: str) -> Any:
        raise NotImplementedError()

    @staticmethod
    def arrays(data: Any, expressions: str, *args, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def array(data: Any, key: str) -> Any:
        raise NotImplementedError()

    @staticmethod
    def contains(data: Any, key: str) -> bool:
        raise NotImplementedError()

    @staticmethod
    def evaluate(data: Any, expression: str, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def counts(data: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def pad(data: Any, length: int, **kwargs: Dict[str, Any]) -> Any:
        raise NotImplementedError()

    @staticmethod
    def flatten(data: Any, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def num_entries(array: Any) -> int:
        raise NotImplementedError()

    @staticmethod
    def sum(data: Any, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def prod(array: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def any(array: Any, **kwargs) -> bool:
        raise NotImplementedError()

    @staticmethod
    def count_nonzero(array: Any, **kwargs) -> Any:
        raise NotImplementedError()

    @staticmethod
    def max(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def min(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def argmax(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def argmin(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def count_zero(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def dtype(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def is_bool(array, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def arrays_as_np_array(data, array_names, **kwargs):
        """
        Takes input data and converts it to an array of numpy arrays.
        e.g. arrays_as_np_array(data, ["x", "y"])
        results in
        array(<array of x>, <array of y>)
        """
        raise NotImplementedError()

    @staticmethod
    def to_pandas(data, keys):
        raise NotImplementedError()

    @staticmethod
    def valid_entry_mask(data: Any) -> Any:
        raise NotImplementedError()

    @staticmethod
    def only_valid_entries(data: Any) -> Any:
        raise NotImplementedError()

    @staticmethod
    def filtered_len(data: Any) -> int:
        raise NotImplementedError()

    @staticmethod
    def fill_none(data: Any, fill_value, **kwargs):
        raise NotImplementedError()

    @staticmethod
    def values_as_type(data: Any, dtype, **kwargs):
        raise NotImplementedError()


class DataIndexProtocol(Protocol):
    def resolve_index(self, index: str) -> str:
        pass


class ArrayLike(Protocol):
    """Placeholder for an array-like object"""
    pass


class FileLike(Protocol):
    """Placeholder for a file-like object"""
    pass


class TreeLike(Protocol):
    """Placeholder for a tree-like object"""
    pass


class DataConnector(MutableMapping):
    """Read-only data connector"""

    _methods: ArrayMethodsProtocol = None

    def __init__(self, **kwargs):
        pass

    def __contains__(self, key: str) -> bool:
        pass

    def get(self, key: str) -> Any:
        pass

    @property
    def num_entries(self) -> int:
        pass

    def array_dict(self, keys: List[str], **kwargs) -> Any:
        pass

    def keys(self) -> List[str]:
        pass


@dataclass
class DatasetConfig:
    # TODO: to be simplified/removed later
    name: str
    eventtype: str


@dataclass
class ConfigProxy:
    # TODO: to be simplified/removed later
    dataset: DatasetConfig
    # raise exception if overwriting existing data or warn if set to False
    fail_on_overwrite: bool = False


class DataWrapperProtocol(Protocol):
    pass


class DoNothingDataWrapper(DataWrapperProtocol):
    def __init__(self, **kwargs):
        pass

    def __call__(self, data):
        return data


class RangedDataWrapper(DataWrapperProtocol):
    def __init__(self, **kwargs):
        self._start = kwargs.get("start", 0)
        self._stop = kwargs.get("stop", None)

    def __call__(self, data):
        return data


class MaskedDataWrapper(DataWrapperProtocol):
    def __init__(self, **kwargs):
        self._mask = kwargs.get("mask", None)

    def __call__(self, data):
        return data


class DataMapping(MutableMapping):
    _connector: DataConnector
    _methods: ArrayMethodsProtocol
    _data_wrappers: List[DataWrapperProtocol]
    _extra_variables: Dict[str, ArrayLike]
    _indices: List[DataIndexProtocol] = field(default_factory=list)
    _config: ConfigProxy

    def __init__(
        self,
        connector: DataConnector,
        methods: ArrayMethodsProtocol,
        data_wrappers: List[DataWrapperProtocol] = None,
        indices=None,
    ):
        self._connector = connector
        self._methods = methods
        if data_wrappers is None:
            self._data_wrappers = [DoNothingDataWrapper()]
        else:
            self._data_wrappers = data_wrappers

        self._extra_variables = {}
        if indices is not None:
            self._indices = indices
        self._config = None

    def __resolve_key__(self, key):
        if key in self._extra_variables:
            return key
        if self._indices:
            lookup = key
            for index in reversed(self._indices):
                lookup = index.resolve_index(lookup)
                if lookup in self._connector:
                    return lookup
        return key

    def __contains__(self, name):
        if name in self._extra_variables:
            return True
        if self._indices:
            lookup = name
            for index in reversed(self._indices):
                lookup = index.resolve_index(lookup)
                if lookup in self._connector:
                    return True

        return name in self._connector

    def __getitem__(self, key):
        if isinstance(key, Sequence) and not isinstance(key, str):
            return self.__getitems__(key)
        if key in self._extra_variables:
            return self._extra_variables[key]
        lookup = key
        for index in reversed(self._indices):
            lookup = index.resolve_index(lookup)
            if lookup in self._connector:
                return self._connector[lookup]
        raise KeyError(f"{key} not found in {self._connector}")

    def __getitems__(self, keys):
        all_string = all(isinstance(key, str) for key in keys)
        if all_string:
            return self.arrays(keys)

    def __setitem__(self, key, value):
        pass

    def __delitem__(self, key):
        pass

    def __iter__(self):
        pass

    def __len__(self):
        return len(self._connector)

    def add_variable(self, name, value):
        if name not in self:
            self._extra_variables[name] = value
        else:
            if self._config.fail_on_overwrite:
                raise ValueError(f"Trying to overwrite existing variable: {name}")
            print(f"Trying to overwrite existing variable: {name} ... skipping")

    def new_variable(self, name, value):
        """Adapter for legacy method"""
        return self.add_variable(name, value)

    def evaluate(self, expression, **kwargs):
        return self._methods.evaluate(self, expression, **kwargs)

    @property
    def num_entries(self):
        return self._connector.num_entries

    @property
    def tree(self):
        return self

    def arrays(self, keys: Sequence, **kwargs) -> List[ArrayLike]:
        if "outputtype" in kwargs:
            # renamed uproot3 -> uproot4
            outputtype = kwargs.pop("outputtype")
            kwargs["how"] = outputtype
        kwargs["how"] = kwargs.get("how", tuple)
        return self._methods.arrays(self, keys, **kwargs)

    def keys(self):
        all_keys = set(self._connector.keys())
        all_keys.update(self._extra_variables.keys())
        return all_keys

    def add_dataset_info(self, dataset_name: str, dataset_eventtype: str) -> None:
        self._config = ConfigProxy(DatasetConfig(dataset_name, dataset_eventtype))

    @property
    def config(self):
        return self._config

    def apply_mask(self, mask):
        self._data_wrappers = [MaskedDataWrapper(mask=mask)] + self._data_wrappers

################
# Processing
################


@dataclass
class ProcessingStepResult:
    data: Any
    error_code: int = 0
    error_message: str = ""
    result: Any = None
    bookkeeping: dict[Any, Any] = field(default_factory=dict)
    rtype: str = None
    reducer = Callable[[Any], Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    filters: dict[str, Any] = field(default_factory=dict)


class ProcessingStep(Protocol):
    """
    A processing step is a callable that takes a dictionary of datasets and returns a dictionary of results.
    A processing step should not write to disk.
    """
    name: str

    def __call__(self, data: ProcessingStepResult) -> ProcessingStepResult:
        ...

    def set_extra(self, **kwargs) -> None:
        for name, value in kwargs.items():
            if f"_{name}" in self.__dict__:
                setattr(self, f"_{name}", value)


class ProcessingBackend(Protocol):
    """
    A processing backend is class that implements the configure and execute methods.
    """

    _config: Dict[str, Any] = {}

    def configure(self, **kwargs: Dict[str, Any]) -> None:
        """
          Passes configuration to the backend.
        """
        pass

    def execute(
        self,
        sequence: Dict[str, ProcessingStep],
        datasets: Dict[str, Any],
        args,
        plugins,
    ) -> Tuple[Any, Any]:
        """
            Executes the processing sequence on the datasets.
        """
        pass


@dataclass
class InputData:
    """
    A class to hold the input data and to provide some helper methods.
    """
    datasets: List[Any]

    @property
    def files_by_dataset(self) -> Dict[str, List[str]]:
        return {d.name: d.files for d in self.datasets}

    @property
    def files(self) -> List[str]:
        return [f for d in self.datasets for f in d.files]

    @staticmethod
    def dataset_type(dataset: Any) -> str:
        return "data" if dataset.name == "data" else "mc"


class Collector():
    """
    A collector is a callable that takes a dictionary of datasets and returns merged results or writes to disk.
    """

    def __call__(self, *results: list[ProcessingStepResult], **kwargs) -> ProcessingStepResult:
        rtypes = [result.rtype for result in results]
        reducible = all([rtype == rtypes[0] for rtype in rtypes])
        reducible = reducible and all([result.reducer is not None for result in results])
        if not reducible:
            raise RuntimeError("Cannot reduce results of different types")
        return reduce(self.reducer, results)
