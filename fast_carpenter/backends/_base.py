from dataclasses import dataclass
from typing import Any, Dict, List, Protocol, Tuple

from ..data_import._base import DataImportPlugin


class ProcessingBackend(Protocol):

    _config: Dict[str, Any] = {}

    def configure(self, **kwargs: Dict[str, Any]) -> None:
        pass

    def execute(
        self, sequence: Dict[str, Any], datasets: Dict[str, Any], args, plugins
    ) -> Tuple[Any, Any]:
        pass


class ResultsCollector(Protocol):
    def collect(self, *args, **kwargs):
        pass


def postprocess(sequence):
    results = {}
    for i, step in enumerate(sequence):
        if not hasattr(step, "collector"):
            continue
        collector: ResultsCollector = step.collector()
        output = collector.collect([(d, (s[i],)) for d, s in sequence.items()])
        results[step.name] = output
    return results


@dataclass
class InputData:
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


class Workflow:
    # TODO: to be moved to fast-flow
    # workflow = fast_flow.from_carpenter_config(carpenter_config)
    # workflow = fast_flow.from_gitlab_ci_config(".gitlab-ci.yml")
    data_import_prefix = "data_import"
    collector_prefix = "collect_results"

    def __init__(self, sequence):
        self.sequence = sequence
        self.task_graph = self.__create_task_graph()
        self.__task_graph = self.task_graph.copy()
        self.final_task = None

    def __create_task_graph(self):
        """Transforms fast-carpenter task list to Dask-like task graph.
        e.g.
        {
            'BasicVars': <fast_carpenter.define.variables.Define object at 0x7fca26d5c640>,
            'DiMuons': <cms_hep_tutorial.DiObjectMass object at 0x7fca26a881c0>,
            'NumberMuons': <fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca26d5c6d0>,
            'EventSelection': <fast_carpenter.selection.stage.CutFlow object at 0x7fca0bfd32b0>,
            'DiMuonMass': <fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca0b770dc0>
        }
        -->
        {
            'BasicVars': (<fast_carpenter.define.variables.Define object at 0x7fca26d5c640>),
            'DiMuons': (<cms_hep_tutorial.DiObjectMass object at 0x7fca26a881c0>, 'BasicVars'),
            'NumberMuons': (<fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca26d5c6d0>, 'DiMuons'),
            'EventSelection': (<fast_carpenter.selection.stage.CutFlow object at 0x7fca0bfd32b0>, 'NumberMuons'),
            'DiMuonMass': (<fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca0b770dc0>, 'EventSelection')
        }
        """
        task_graph = {}
        previous = None

        def task_wrapper(task):
            return task.event

        for task_name, task in self.sequence.items():
            if previous is not None:
                task_graph[task_name] = (task_wrapper(task), previous)
            else:
                task_graph[task_name] = (task_wrapper(task), "data_import")
            previous = task_name
        task_graph[self.collector_prefix] = (postprocess, previous)
        return task_graph

    def add_data_stage(
        self, data_import_plugin: DataImportPlugin, input_data: InputData
    ) -> None:
        """Adds data stage to the workflow. One node is generated for each file in each dataset"""
        task_graph = {}
        for dataset in input_data.datasets:
            dataset_suffix = dataset.name
            for i, file_name in enumerate(dataset.files):
                file_suffix = f"file-{i}"
                task_suffix = f"{dataset_suffix}-{file_suffix}"
                task_graph[f"{self.data_import_prefix}-{task_suffix}"] = (
                    data_import_plugin.open,
                    [file_name],
                )
                for task, (payload, previous) in self.__task_graph.items():
                    if previous == self.data_import_prefix:
                        previous = f"{self.data_import_prefix}-{task_suffix}"
                    else:
                        previous = f"{previous}-{task_suffix}"
                    task_graph[f"{task}-{task_suffix}"] = (
                        payload,
                        previous,
                    )
        self.task_graph = task_graph

        return self

    def add_collector(self) -> None:
        def final_collector(inputs: List[Any]) -> List[Any]:
            # TODO: merge results from all datasets
            # TODO: if there are more than X results, merge in multiple steps
            return inputs

        final_task = final_collector
        collection_tasks = [
            task
            for task in self.task_graph.keys()
            if task.startswith(self.collector_prefix)
        ]
        self.task_graph["__reduce__"] = (final_task, collection_tasks)
        self.final_task = final_task
        return self

    def to_image(self, output_file: str) -> None:
        import dask
        from dask.delayed import Delayed

        delayed_dsk = Delayed("w", self.task_graph)
        dask.visualize(delayed_dsk, filename=output_file, verbose=True)

    def to_gitlab_ci_yaml(self, output_file: str) -> None:
        pass

    def to_parsl(self) -> None:
        # 1. turn all data-import tasks into python apps
        # 2. turn all other tasks into parsl tasks
        # add final task and construct parsl graph
        pass

    def to_dask(self) -> None:
        pass

    def __repr__(self) -> str:
        output = ""
        for task, description in self.task_graph.items():
            output += f"{description[1]} -> {task} \n"
        return output
