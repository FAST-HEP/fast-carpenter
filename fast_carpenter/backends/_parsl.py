import concurrent.futures
from dataclasses import dataclass

import parsl
from parsl.app.app import python_app
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider

from fast_carpenter.tree_adapter import TreeLike, create_masked

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
                # address="127.0.0.0"
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


class Workflow:
    def __init__(self, sequence):
        self.sequence = sequence
        self.task_graph = self.create_task_graph()

    def create_task_graph(self):
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
            return python_app(task.event)

        for task_name, task in self.sequence.items():
            if previous is not None:
                task_graph[task_name] = (task_wrapper(task), previous)
            else:
                task_graph[task_name] = (task_wrapper(task), "chunk")
            previous = task_name
        return task_graph


class ParslConnector:
    _data: TreeLike
    _config: Config

    def __init__(self, data, config=None):
        self._data = data
        self._config = config or default_cfg()


@dataclass
class ConfigProxy:
    name: str
    eventtype: str

    @property
    def dataset(self):
        return self


@dataclass
class Chunk:
    tree: TreeLike
    config: ConfigProxy


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
        for task, payload in workflow.task_graph.items():
            print(task, payload)
        # import dask
        # from dask.delayed import Delayed
        # # print(workflow.task_graph)
        # delayed_dsk = Delayed("w", workflow.task_graph)
        # dask.visualize(delayed_dsk, filename='carpenter.png', verbose=True)
        return (0, 0)
        # for now simple config:
        _parsl_initialize()

        # for each dataset execute the sequence
        files = datasets[0].files
        tree_name = datasets[0].tree  # [0]
        results = prepare(sequence)
        # results = build_compute_graph(sequence)

        import uproot

        config = ConfigProxy(
            name=datasets[0].name,
            eventtype="data" if datasets[0].name == "data" else "mc",
        )
        outputs = {}
        with uproot.open(files[0]) as f:
            tree = f[tree_name]
            masked = create_masked({"tree": tree})

            chunk = Chunk(masked, config)
            for _, (step_name, future) in enumerate(zip(sequence, results.values())):
                try:
                    result = future(chunk).result()
                    print(result)
                except Exception as e:
                    print(e)
                else:
                    step = sequence[step_name]
                    if not hasattr(step, "collector"):
                        continue
                    collector = step.collector()
                    output = collector.collect([(step_name, (step,))])
                    outputs[step_name] = output
            # outputs = [result(chunk).result() for result in results.values()]
        # outputs = postprocess(sequence).result()
        _parsl_stop()

        return outputs, "stages"
