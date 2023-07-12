from collections import namedtuple
import numpy as np
from typing import Any

from ..workflow import Task, TaskCollection

FakeEventRange = namedtuple("FakeEventRange", "start_entry stop_entry entries_in_block")


class Namespace():
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeBEEvent(object):
    def __init__(self, tree: Any, eventtype: str):
        self.tree = tree
        self.config = Namespace(dataset=Namespace(eventtype=eventtype))

    def __len__(self) -> int:
        return len(self.tree)

    def count_nonzero(self) -> int:
        return self.tree.count_nonzero()

    def arrays(self, array_names: list[str], library: str = "np", outputtype: Any = list) -> list[Any]:
        return [self.tree[name] for name in array_names]


class FakeTree(dict):
    length: int = 101

    def __init__(self, length: int = 101):
        super(FakeTree, self).__init__(
            NMuon=np.linspace(0, 5., length),
            NElectron=np.linspace(0, 10, length),
            NJet=np.linspace(2, -18, length),
        )
        self.length = length

    def __len__(self) -> int:
        return self.length

    def arrays(self, array_names: list[str], library: str = "np", outputtype=list) -> list[Any]:
        return [self[name] for name in array_names]


class DummyMapping(dict):
    pass


def execute_task(task: Task):
    """Execute a task by calling it with its arguments."""
    return task[0](*task[1:])

# Although closed, this issue is still relevant:
# https://github.com/dask/dask/issues/3741
# it prevents us from using the threaded scheduler for testing
# as it does not support keyword arguments.
#
# def execute_tasks(tasks: TaskCollection) -> Any:
#     from dask.threaded import get
#     try:
#         return get(tasks.graph, tasks.last_task)
#     except KeyError as e:
#         raise KeyError(f"A task failed to execute: {e}")


def execute_tasks(tasks: TaskCollection) -> Any:
    """Execute a task collection using the distributed scheduler."""
    from dask.distributed import Client

    client = Client()
    try:
        result = client.get(tasks.graph, tasks.last_task)
        return result
    except KeyError as e:
        raise KeyError(f"A task failed to execute: {e}")
    finally:
        client.close()


def visualize_tasks(tasks: TaskCollection, filename: str) -> None:
    """Visualize a task graph."""
    from dask.delayed import Delayed
    dsk_delayed = Delayed("w", tasks.graph)
    dsk_delayed.visualize(filename=filename, verbose=True, engine="graphviz")
