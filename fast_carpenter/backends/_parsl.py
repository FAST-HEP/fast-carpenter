import concurrent.futures

import parsl
from parsl.app.app import python_app
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

from fast_carpenter.tree_adapter import TreeLike

from ._base import InputData, ProcessingBackend, Workflow


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
