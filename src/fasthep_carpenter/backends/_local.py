"""
Backend for local execution of a workflow.
Intended for testing and debugging.
"""
from typing import Any

from ..settings import Settings
from ..workflow import Workflow

class NewProcessingBackend():

    def execute(self, workflow: Workflow, settings: Settings) -> tuple[Any]:
        raise NotImplementedError("This method needs to be implemented by the backend")


class LocalBackend(NewProcessingBackend):

    def execute(self, workflow: Workflow, settings: Settings) -> tuple[Any]:
        workflow.visualize("local-workflow.png")
        # we cheat here a bit and use Dask
        from dask.distributed import Client, performance_report
        client = Client()
        # with performance_report(filename="dask-report.html"):
        result = client.get(workflow.graph, workflow.last_task)
        return result