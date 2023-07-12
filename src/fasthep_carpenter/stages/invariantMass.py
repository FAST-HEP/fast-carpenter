"""
    Example of a specialized stage that defines new variables.
    This stage is not necessarily useful, but it shows how to create a custom stage.
"""

from typing import Any

import awkward as ak
import numpy as np
import vector

from fasthep_logging import get_logger

from ..protocols import DataMapping, ProcessingStep, ProcessingStepResult

from ..workflow import Task, get_task_number, TaskCollection

log = get_logger("FASTHEP::Carpenter")


def create_4_vector(vector_data) -> Any:
    """
    Creates a 4-vector from the given components.
    """

    px, py, pz, energy = vector_data
    return vector.zip({
        "px": px,
        "py": py,
        "pz": pz,
        "energy": energy,
    })


def create_combinations(data, n: int, fields) -> (ak.Array | ak.Record):
    with np.errstate(invalid="ignore"):
        return ak.combinations(data, n, fields=fields)


def invariant_mass(combinations: ak.Array) -> ak.Array:
    return (combinations.p1 + combinations.p2).mass


class InvariantMass(ProcessingStep):

    _name: str
    _collection: dict[str, Any]
    _output: str
    _tasks: TaskCollection
    multiplex: bool = True

    def __init__(self, name: str, collection: dict[str, Any], output: str, **kwargs) -> None:
        self._name = name
        self._collection = collection
        self._output = output
        self._tasks = None

    def __call__(self, data: ProcessingStepResult, name: str, value: Any) -> ProcessingStepResult:
        """"""
        log.trace(f"Processing {name=} in stage {self._name}")
        data.data.add_variable(name, value)
        data.bookkeeping[(self.__class__.__name__, self._name)] = self.__dask_tokenize__()
        return data

    def __dask_tokenize__(self):
        return (InvariantMass, self._name, self._collection, self._output)

    @property
    def name(self) -> str:
        return self._name

    def tasks(self, data_source: str = "__join__") -> TaskCollection:
        """
        Uses the colellections to create a task graph.
        """
        if self._tasks is not None:
            return self._tasks

        self._tasks = TaskCollection()
        fourVector = self._collection["fourVector"]
        mask = self._collection.get("mask", None)

        # get collection data - apply mask if present
        def local_data_source(source):
            return source.data if isinstance(source, ProcessingStepResult) else source

        def get_vector_data(data: DataMapping, fourVector: tuple[str], mask: str) -> tuple[Any]:
            if mask is None:
                return data.arrays(fourVector, how=tuple)
            else:
                arrays = data.arrays(fourVector, how=tuple)
                mask_data = data[mask]
                return (ak.mask(a, mask_data) for a in arrays)

        local_ds_name = f"__data_source__stage__{self.__class__.__name__}__{self.name}"
        task_id = get_task_number(local_ds_name)
        local_ds_name = f"{local_ds_name}-{task_id}"
        self._tasks.add_task(local_ds_name, local_data_source, data_source)

        vector_source = f"__{self.__class__.__name__}_get_vector_data"
        task_id = get_task_number(vector_source)
        vector_source = f"{vector_source}-{task_id}"
        # partial_func = partial(get_vector_data, fourVector=fourVector, mask=mask)
        self._tasks.add_task(vector_source, get_vector_data, local_ds_name, fourVector, mask)

        # create vector of 4-momenta
        task_name = f"__{self.__class__.__name__}_create_4_vector"
        task_id = get_task_number(task_name)
        task_name = f"{task_name}-{task_id}"
        self._tasks.add_task(task_name, create_4_vector, vector_source)
        previous_task = task_name

        # create combinations (ak.combinations)
        task_name = f"__{self.__class__.__name__}_combinations"
        task_id = get_task_number(task_name)
        task_name = f"{task_name}-{task_id}"
        self._tasks.add_task(task_name, create_combinations, previous_task, 2, ["p1", "p2"])
        previous_task = task_name

        # calculate invariant mass
        # bug: dask performance report has an issue with this: Sizeof calculation failed
        task_name = f"__{self.__class__.__name__}_invariant_mass"
        task_id = get_task_number(task_name)
        task_name = f"{task_name}-{task_id}"
        self._tasks.add_task(task_name, invariant_mass, previous_task)
        previous_task = task_name

        # add to output
        task_name = f"__{self.__class__.__name__}_add_to_output"
        task_id = get_task_number(task_name)
        task_name = f"{task_name}-{task_id}"
        self._tasks.add_task(task_name, self, data_source, self._output, previous_task)

        return self._tasks
