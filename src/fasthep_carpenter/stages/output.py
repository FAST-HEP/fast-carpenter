"""Stages that produce output"""

import os

from fasthep_logging import get_logger
from ..protocols import ProcessingStep, ProcessingStepResult
from ..workflow import get_task_number, TaskCollection
from ..settings import Settings

log = get_logger("FASTHEP::Carpenter")

VariableDefinition = dict[str, str]


class FileOutput(ProcessingStep):
    """Creates output based on configured output variables.
    File output is relative to the output directory.

    example:

    file_output:
      output_type: "ROOT|Parquet|CSV|text"
      path: "output.root|output.parquet|output.csv|output.txt"
      content:
        - NGoodMuons
        - Muon_pt
        - __histogram__*

    """

    _name: str
    _output_type: str
    _path: str
    _content: list[str]
    _tasks: TaskCollection
    _settings: Settings = None

    multiplex: bool = True

    def __init__(self, name: str, output_type: str, path: str, content: list[str], **kwargs) -> None:
        self._name = name
        self._output_type = output_type
        self._path = path  # TODO: get output directory from settings
        self._content = content
        self._tasks = None
        self._settings = None

    def __call__(self, data: ProcessingStepResult, path: str) -> ProcessingStepResult:
        import awkward as ak
        with open(path, "w") as f:
            content = data.data.arrays(self._content)
            for item in content:
                f.write(ak.to_json(item))
        # TODO: add bookkeeping info and reducer
        return data

    @property
    def name(self) -> str:
        return self._name

    def tasks(self, data_source: str = "__join__") -> TaskCollection:
        if self._tasks:
            return self._tasks

        self._tasks = TaskCollection()
        output_path = os.path.join(self._settings.outdir, self._path)

        task_name = f"file_output_{self._name}"
        task_id = get_task_number(task_name)
        task_name = f"{task_name}-{task_id}"

        # insert task number into path just before file extension
        output_path = os.path.splitext(output_path)[0] + f"_{task_id}" + os.path.splitext(output_path)[1]

        self._tasks.add_task(task_name, self, data_source, output_path)

        return self._tasks


class ConsoleOutput(ProcessingStep):
    """Creates output based on configured output variables.
    File output is relative to the output directory.

    example:

    console_output:
      path: "stdout|log"
      content:
        - NGoodMuons
        - Muon_pt
    """

    _name: str
    _path: str
    _content: list[str]

    def __init__(self, name: str, output_type: str, path: str, content: list[str], **kwargs) -> None:
        self._name = name
        self._output_type = output_type
        self._path = path
        self._content = content
