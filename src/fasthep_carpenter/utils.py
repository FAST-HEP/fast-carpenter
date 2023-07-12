import hashlib
import os
from pathlib import Path
from typing import Any, Dict


def mkdir_p(path: Path | str) -> None:
    os.makedirs(path, exist_ok=True)


def list_python_packages_with_versions() -> dict[str, str]:
    import pkg_resources

    return {
        dist.project_name: dist.version
        for dist in pkg_resources.working_set
        if dist.project_name != "pip"
    }


def register_in_collection(
    collection: Dict[str, Any], collection_name: str, name: str, obj: Any
) -> None:
    if name in collection:
        raise ValueError(f"{collection_name} {name} already registered.")
    collection[name] = obj


def unregister_from_collection(
    collection: Dict[str, Any], collection_name: str, name: str, obj: Any
) -> None:
    if name not in collection:
        raise ValueError(f"{collection_name} {name} not registered.")
    collection[name].pop()


def string_to_short_hash(string: str) -> str:
    """Convert a string to a short hash."""
    return hashlib.sha1(string.encode()).hexdigest()[:8]


def combine_weights(weights: dict[str, Any]) -> Any:
    from functools import reduce
    import operator

    return reduce(operator.mul, weights.values(), 1.0)


def broadcast_weights(arrays: dict[str, Any], weights: dict[str, Any]) -> dict[str, Any]:
    import awkward as ak

    weight = combine_weights(weights)
    return {k: ak.broadcast_arrays(weight, v)[0] for k, v in arrays.items()}


def flatten_and_remove_none(arrays: dict[str, Any]) -> dict[str, Any]:
    import awkward as ak
    arrays = {k: ak.flatten(v, axis=1) for k, v in arrays.items()}
    arrays = {k: v[~ak.is_none(v)] for k, v in arrays.items()}
    return arrays
