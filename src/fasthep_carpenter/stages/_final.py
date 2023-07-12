"""Final stage of the Carpenter workflow."""

from ..protocols import ProcessingStep, ProcessingStepResult
from ..settings import Settings


class Final(ProcessingStep):

    settings: Settings = None

    def __init__(self) -> None:
        self._settings = None

    def __call__(self, *args: list[ProcessingStep]) -> ProcessingStepResult:
        # 1. Loop over all results
        # 2. Use supplied reducer to combine them
        # 3. Produce report
        # 4. Return the result
        return args
