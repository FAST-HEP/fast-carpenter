"""
Provides a common interface to select a backend

Each backend is wrapped in a function so that it is only imported if requested
"""
from functools import partial


def get_alphatwirl():
    from . import _alphatwirl
    return _alphatwirl


def get_coffea():
    from . import coffea
    return coffea

def get_parsl(processing_mode):
    from ._parsl import ParslBackend
    backend = ParslBackend()
    backend.configure(processing_mode=processing_mode)
    return backend

KNOWN_BACKENDS = {
    "multiprocessing": get_alphatwirl,
    "htcondor": get_alphatwirl,
    "sge": get_alphatwirl,
    "coffea:local": get_coffea,
    "coffea:parsl": get_coffea,
    "coffea:dask": get_coffea,
    "parsl:local": partial(get_parsl, "local"),
    "parsl:htcondor": partial(get_parsl, "local"),
    "parsl:dirac": partial(get_parsl, "local"),
}

KNOW_BACKENDS_NAMES = ", ".join(list(KNOWN_BACKENDS.keys()))


def get_backend(name):
    if name not in KNOWN_BACKENDS:
        raise ValueError(f"Unknown backend requested, '{name}'. Known backends: {KNOW_BACKENDS_NAMES}")
    return KNOWN_BACKENDS[name]()
