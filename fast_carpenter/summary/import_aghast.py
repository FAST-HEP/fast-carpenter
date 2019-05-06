try:
    import aghast
    has_aghast = True
except ImportError as ex:
    if "aghast" not in str(ex):
        raise

    class AghastCatcher:
        def __getattr__(self, attr):
            msg = "Aghast is not installed but has been needed."
            msg += "\nInstall it using conda:"
            msg += "\n\n       conda install aghast"
            raise ImportError(msg)

    aghast = AghastCatcher()
    has_aghast = False
