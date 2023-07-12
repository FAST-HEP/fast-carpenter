from typing import Any

from fasthep_logging import get_logger

from ..protocols import ProcessingStep, ProcessingStepResult
from ..utils import broadcast_weights, flatten_and_remove_none

log = get_logger("FASTHEP::Carpenter")

Binning = dict[str, tuple[int, float, float]]

class Histogram(ProcessingStep):
    _name: str
    _prefix: str = "__histogram__"
    _binning: Binning
    _weights: dict[str, str] | None
    multiplex: bool = True

    def __init__(
        self,
        name: str,
        binning: Binning,
        weights: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        self._name = self._prefix + name
        self._binning = binning
        self._weights = weights

    def __call__(self, data: ProcessingStepResult) -> ProcessingStepResult:
        import awkward as ak

        log.trace(f"Processing stage {self._name}")
        data.bookkeeping[
            (self.__class__.__name__, self._name)
        ] = self.__dask_tokenize__()

        import hist

        axes = []
        for name, (bins, low, high) in self._binning.items():
            axes.append(hist.axis.Regular(bins, low, high, name=name))
        histogram = hist.Hist(
            *axes,
            hist.storage.Weight(),
        )
        arrays = data.data.arrays(list(self._binning.keys()), how=dict)

        # extend weights to the same length as the data
        weight_values = broadcast_weights(
            arrays, data.data.arrays(self._weights, how=dict)
        )
        # remove empty entries and flatten
        arrays = flatten_and_remove_none(arrays)
        weight_values = flatten_and_remove_none(weight_values)
        weight = weight_values.popitem()[1]

        histogram.fill(**arrays, weight=weight)
        if data.result is None:
            data.result = {}
        data.result[self.name] = histogram

        return data

    def __dask_tokenize__(self):
        return (Histogram, self._name, self._binning, self._weights)

    @property
    def name(self) -> str:
        return self._name
