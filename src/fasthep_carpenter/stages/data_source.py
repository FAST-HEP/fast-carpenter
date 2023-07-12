from functools import partial

from ..protocols import DataMapping, ProcessingStep, InputData, ProcessingStepResult
from ..settings import Settings
from ..workflow import TaskCollection, TaskGraph, get_task_number
from ..data_import import DataImportPlugin, get_data_import_plugin
from ..data_mapping import create_mapping


class DataSource(ProcessingStep):
    """Imports data from a file or other source."""

    _name: str
    _data_import: str
    _paths: list[str]
    _data_import_plugin: DataImportPlugin
    _datasets: InputData
    _settings: Settings
    _tasks: TaskCollection
    _require_multiplex: bool = False
    multiplex: bool = False

    def __init__(self, name, data_import, paths, **kwargs) -> None:
        self._name = name
        self._data_import = data_import
        self._paths = paths

        self._data_import_plugin = get_data_import_plugin(data_import)

        self._datasets = None
        self._settings = None
        self._tasks = None

    def __call__(self, input_file_name: str, dataset: str) -> ProcessingStepResult:
        input_file = self._data_import_plugin.open([input_file_name])
        data = create_mapping(
            input_file,
            treenames=self._paths,
            methods=self._data_import,
            connector="tree" if len(self._paths) > 1 else "file",
        )

        return ProcessingStepResult(
            data=data,
            bookkeeping={(self.__class__.__name__, self._name): self.__dask_tokenize__()},
            metadata={"dataset": dataset, "input_file": input_file_name},
        )

    def __dask_tokenize__(self):
        return (DataSource, self._name, self._data_import, self._paths)

    def __create_tasks(self) -> None:
        self._tasks = TaskCollection()
        for dataset in self._datasets:
            name_tmp = f"read-data-{dataset.name}"
            for input_file_name in dataset.files:
                task_id = get_task_number(name_tmp)
                task_name = f"{name_tmp}-{task_id}"
                self._tasks.add_task(task_name, self, input_file_name, dataset)

    def tasks(self, data_source: str = None) -> TaskCollection:
        if not self._tasks:
            self.__create_tasks()
        return self._tasks

    @property
    def name(self) -> str:
        return self._name
