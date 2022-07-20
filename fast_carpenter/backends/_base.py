from typing import Any, Dict, Protocol, Tuple


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
