from typing import Any


SUPPORTED_COLLECTORS = {
    "file_merger": "fast_carpenter.v1.collectors.file_merger.FileMerger",
}

class FileMerger():
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        pass


class HistogramCollector():
    def __call__(self, *args: Any, **kwds: Any) -> Any:
        pass

