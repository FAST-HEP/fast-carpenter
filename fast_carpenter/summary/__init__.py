from .binned_dataframe import BinnedDataframe
try:
    from .aghast import BuildAghast
except ImportError as ex:
    if "aghast" not in str(ex):
        raise

    class BuildAghast:
        def __init__(self, name, *arg, **kwargs):
            msg = "Aghast is not installed, but was requested for %s" % name
            msg += "\nInstall it using conda:"
            msg += "\n\n       conda install aghast"
            raise RuntimeError(msg)


__all__ = ["BuildAghast", "BinnedDataframe"]
