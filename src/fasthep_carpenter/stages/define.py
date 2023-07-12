"""Stages for defining new variables"""
from typing import Any

from fasthep_logging import get_logger
from ..protocols import DataMapping, ProcessingStep, ProcessingStepResult
from ..expressions import expression_to_ast
from ..workflow import Task, get_task_number, TaskCollection

log = get_logger("FASTHEP::Carpenter")

VariableDefinition = dict[str, str]


class Define(ProcessingStep):
    """Creates new variables using a string-based expression.

    Example:
      ::

        variables:
          - Muon_pt: "sqrt(Muon_px**2 + Muon_py**2)"
          - Muon_is_good: (Muon_iso > 0.3) & (Muon_pt > 10)
          - NGoodMuons: "count_nonzero(Muon_is_good)"
    """

    _name: str
    _variable_definitions = list[VariableDefinition]
    _variables: list[tuple[str]]
    _tasks: TaskCollection
    multiplex: bool = True

    def __init__(self, name: str, variables: list[VariableDefinition], **kwargs) -> None:
        self._name = name
        self._variable_definitions = variables
        self._variables = []
        for variable in variables:
            for name, expression in variable.items():
                self._variables.append((name, expression))
        self._tasks = None

    def __call__(self, data: ProcessingStepResult, name: str, value: Any) -> ProcessingStepResult:
        log.trace(f"Processing {name=} in stage {self._name}")
        data.data.add_variable(name, value)
        data.bookkeeping[(self.__class__.__name__, self._name)] = self.__dask_tokenize__()
        return data

    def __dask_tokenize__(self):
        return (Define, self._name, self._variable_definitions)

    @property
    def name(self) -> str:
        return self._name

    def tasks(self, data_source: str = "__join__") -> TaskCollection:
        """
        Uses the variable definitions to create a task graph.

        _variables = [
          ("Muon_pt", "sqrt(Muon_px**2 + Muon_py**2)"),
          ("Muon_is_good": "(Muon_iso > 0.3) & (Muon_pt > 10)"),
          ("NGoodMuons": "count_nonzero(Muon_is_good)"),
        ]
        Should create at least 3 tasks to evaluate the expressions, and then
        3 tasks to add the variables to the data mapping.
        Each definition should depend on the previous one.

        Returns:
            dict[str, Task]: A dictionary of tasks to be executed.
        e.g.
        {"define-Muon_pt": (data.add_variable, "Muon_pt", ...),
            "define-Muon_is_good": (data.add_variable, "Muon_is_good", ..., "define-Muon_pt"),
        """
        if self._tasks:
            return self._tasks

        self._tasks = TaskCollection()

        previous_definition = None
        # previous_variable = None

        def local_data_source(source):
            return source.data if isinstance(source, ProcessingStepResult) else source

        local_ds_name = f"__data_source__stage__{self.__class__.__name__}__{self.name}"
        task_id = get_task_number(local_ds_name)
        local_ds_name = f"{local_ds_name}-{task_id}"
        self._tasks.add_task(local_ds_name, local_data_source, data_source)

        for i, (name, expression) in enumerate(self._variables):
            # TODO: move to task builder
            task_name = f"define-{self.name}-{i}-{name}"
            task_id = get_task_number(task_name)
            task_name = f"{task_name}-{task_id}"
            ast_wrapper = expression_to_ast(expression)

            # if this is the 2nd or later variable, add the previous variable as the data source
            if previous_definition:
                new_source = f"__data_source__{previous_definition}"
                self._tasks.add_task(new_source, local_data_source, previous_definition)
                tasks = ast_wrapper.to_tasks(new_source)
            else:
                tasks = ast_wrapper.to_tasks(local_ds_name)
            self._tasks.update(tasks)

            if previous_definition:
                self._tasks.add_task(task_name, self, previous_definition, name, tasks.last_task)
            else:
                self._tasks.add_task(task_name, self, data_source, name, tasks.last_task)
            previous_definition = task_name

        return self._tasks
