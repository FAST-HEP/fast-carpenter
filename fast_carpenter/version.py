"""Defines version of codebase
"""


def split_version(version):
    """Split a semantic version string into a version_info tuple
    """
    result = (version,)
    for div in ".-":
        result = [tok.split(div) for tok in result]
        result = sum(result, [])
    return tuple(result)


__version__ = '0.19.1'
version_info = split_version(__version__) # noqa
