from . import alphatwirl


known_backends = {"alphatwirl": alphatwirl}


def get_backend(name):
    if name not in known_backends:
        raise ValueError("Unknown backend requested, '%s'" % name)
    return known_backends[name]
