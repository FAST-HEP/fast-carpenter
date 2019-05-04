from __future__ import absolute_import
from .binned_dataframe import BinnedDataframe
try:
    from .aghast import BuildAghast
except ImportError as ex:
    if "aghast" not in str(ex):
        raise

    class BuildAghast:
        def __init__(self, name, *arg, **kwargs):
            raise RuntimeError("Aghast is not installed, but was requested for %s" % name)


__all__ = ["BuildAghast", "BinnedDataframe"]
