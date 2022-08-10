from typing import List

from dask.distributed import Client, performance_report

from ._base import InputData, ProcessingBackend, ProcessingStep, Workflow


def configure_dask(client, **kwargs):

    kwargs.setdefault("processes", False)
    kwargs.setdefault("threads_per_worker", 2)
    kwargs.setdefault("n_workers", 2)
    kwargs.setdefault("memory_limit", "1GB")

    if client:
        return Client(client)
    else:
        return Client(**kwargs)


class DaskBackend(ProcessingBackend):
    def __init__(self):
        print("Using experimental Dask backend")
        print("There be dragons")

    def configure(self, **kwargs):
        # set sensible defaults
        self._config.update(kwargs)

    def execute(self, sequence: List[ProcessingStep], datasets, args, plugins):
        dask_client = configure_dask(client=None)
        # sequence is a list of steps, we need a dict of steps
        sequence = {s.name: s for s in sequence}
        workflow = Workflow(sequence)
        workflow.to_image("carpenter.png")

        input_data = InputData(datasets)
        data_import_plugin = plugins.get("data_import")
        workflow = workflow.add_data_stage(data_import_plugin, input_data)
        workflow.to_image("carpenter-with-data-import.png")
        workflow.add_collector()
        workflow.to_image("carpenter-with-data-import-and-collector.png")

        with performance_report(filename="dask-report.html"):
            dask_client.get(workflow.task_graph, "__reduce__")
        # TODO: final selection (combining all selection steps) is not correct

        dask_client.close()
        return (0, 0)
