from dataclasses import dataclass
import parsl

from parsl.app.app import python_app

from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor


from ._base import ProcessingBackend
from fast_carpenter.tree_adapter import create_masked, TreeLike

_default_cfg = Config(
    executors=[
        HighThroughputExecutor(
            label="carpenter_parsl_default",
            cores_per_worker=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
            ),
            # address="127.0.0.0"
        )
    ],
    strategy=None,
)


def _parsl_initialize(config=None):
    parsl.clear()
    parsl.load(config)


def _parsl_stop():
    parsl.dfk().cleanup()
    parsl.clear()


def prepare(sequence):
    futures = [python_app(s.event) for s in sequence]
    return futures


@dataclass
class Chunk():
    tree: TreeLike


class ParslBackend(ProcessingBackend):

    def __init__(self):
        print("Using experimental Parsl backend")
        print("There be dragons")

    def configure(self, **kwargs):
        # set sensible defaults
        self._config.update(kwargs)

    def execute(self, sequence, datasets, args, plugins):
        # for now simple config:
        _parsl_initialize(_default_cfg)

        # for each dataset execute the sequence
        files = datasets[0].files
        tree_name = datasets[0].tree[0]
        results = prepare(sequence)

        import uproot
        with uproot.open(files[0]) as f:
            tree = f[tree_name]
            masked = create_masked({"tree": tree})

            chunk = Chunk(masked)

            for result in results:
                result(chunk).result()

        _parsl_stop()

        return "results", "stages"
