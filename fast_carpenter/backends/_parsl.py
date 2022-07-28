import concurrent.futures
from dataclasses import dataclass
from typing import Any, Dict, List

import parsl
from parsl.app.app import python_app
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

from fast_carpenter.data_import._base import DataImportPlugin
from fast_carpenter.tree_adapter import TreeLike

from ._base import ProcessingBackend


def default_cfg():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="carpenter_parsl_default",
                cores_per_worker=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
                max_workers=1,
                address="127.0.0.0",
            )
        ],
        strategy=None,
    )


def _parsl_initialize(config=None):
    if config is None:
        config = default_cfg()
    parsl.clear()
    parsl.load(config)


def _parsl_stop():
    parsl.dfk().cleanup()
    parsl.clear()


def prepare(sequence):
    futures = {k: python_app(v.event) for k, v in sequence.items()}
    return futures


def _futures_handler(futures):
    return concurrent.futures.as_completed(futures.values())


# @python_app
# def derive_chunks(filename, data_import_plugin, dataset, chunk_size):
#     tree = data_import_plugin.open([filename])
#     n_entries = tree.num_entries
#     n_chunks = n_entries // chunk_size +1
#     return (
#         dataset,
#         data_import_plugin,
#         [(filename, chunk_size, i) for i in range(n_chunks)],
#     )

# def _parsl_get_chunks(filelist, chunk_size):
#     futures = set(
#         derive_chunks(filename, data_import_plugin, dataset, chunk_size) for dataset, filename, data_import_plugin in filelist
#     )

#     outputs = []
#     def chunk_accumulator(total, result):
#         dataset, data_import_plugin, chunks = result
#         for chunk in chunks:
#             total.append((dataset, chunk[0], data_import_plugin, chunk[1], chunk[2]))

#     return futures

# def build_compute_graph(sequence):
#     beginning = python_app(sequence[0].event)
#     if len(sequence) == 1:
#         return [beginning]
#     end = python_app(sequence[-1].event)
#     if len(sequence) == 2:
#         return [beginning, end]
#     graph = end
#     for s in reversed(sequence[1:-2]):
#         print(s.event)
#         graph = graph(join_app(s.event))
#     return graph


@python_app
def run_sequence(sequence, chunk):
    results = [s.event(chunk) for s in sequence]
    return results


@python_app
def postprocess(sequence):
    results = {}
    for i, step in enumerate(sequence):
        if not hasattr(step, "collector"):
            continue
        collector = step.collector()
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


class ParslWorkflow:
    def __init__(self, workflow: Workflow):
        self.workflow = workflow
        self.task_graph = self.workflow.task_graph
        self.final_task = None


class ParslConnector:
    _data: TreeLike
    _config: Config

    def __init__(self, data, config=None):
        self._data = data
        self._config = config or default_cfg()


class ParslBackend(ProcessingBackend):
    def __init__(self):
        print("Using experimental Parsl backend")
        print("There be dragons")

    def configure(self, **kwargs):
        # set sensible defaults
        self._config.update(kwargs)

    def execute(self, sequence, datasets, args, plugins):
        # sequence is a list of steps, we need a dict of steps
        sequence = {s.name: s for s in sequence}
        workflow = Workflow(sequence)
        workflow.to_image("carpenter.png")

        _parsl_initialize()

        input_data = InputData(datasets)
        data_import_plugin = plugins.get("data_import")
        workflow = workflow.add_data_stage(data_import_plugin, input_data)
        workflow.to_image("carpenter-with-data-import.png")
        workflow.add_collector()
        workflow.to_image("carpenter-with-data-import-and-collector.png")
        workflow.final_task().result()
        # TODO: convert workflow to Parsl

        _parsl_stop()
        return (0, 0)
