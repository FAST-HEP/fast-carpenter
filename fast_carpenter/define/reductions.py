import numpy as np
import six
from typing import List

from ..tree_adapter import ArrayMethods

__all__ = ["get_pandas_reduction"]


class BadReductionConfig(Exception):
    pass


class JaggedNth(object):
    def __init__(self, index, fill_missing, force_float=True):
        self.index = index
        self.fill_missing = fill_missing
        self.dtype = None
        if fill_missing is True or fill_missing is False:
            self.dtype = np.bool8
        elif force_float or isinstance(fill_missing, float):
            self.dtype = np.float64
        else:
            self.dtype = np.int32

    def __call__(self, array):
        result = ArrayMethods.pad(array, abs(self.index) + int(self.index >= 0))
        result = ArrayMethods.fill_none(result, self.fill_missing)
        if self.dtype is not None:
            result = ArrayMethods.values_as_type(result, self.dtype)
        return result[..., self.index]


class JaggedMethod(object):
    SUPPORTED: List[str] = ["sum", "prod", "any", "all", "count_nonzero",
                            "max", "min", "argmin", "argmax"]
    DEFAULTS = {
        "sum": {"axis": 1},
    }

    def __init__(self, method):
        self.method_name = method
        self._defaults = self.DEFAULTS.get(method, {})

    def __call__(self, array):
        return getattr(ArrayMethods, self.method_name)(array, **self._defaults)


class JaggedProperty(object):
    SUPPORTED: List[str] = ["counts"]

    def __init__(self, prop_name):
        self.prop_name = prop_name

    def __call__(self, array):
        return getattr(ArrayMethods, self.prop_name)(array)


def get_awkward_reduction(stage_name, reduction, fill_missing=np.nan):
    if isinstance(reduction, six.integer_types):
        return JaggedNth(int(reduction), fill_missing)

    if not isinstance(reduction, six.string_types):
        msg = "{}: requested reduce method is not a string or an int"
        raise BadReductionConfig(msg.format(stage_name))

    if reduction in JaggedMethod.SUPPORTED:
        return JaggedMethod(reduction)
    if reduction in JaggedProperty.SUPPORTED:
        return JaggedProperty(reduction)

    msg = "{}: Unknown method to reduce: '{}'"
    raise BadReductionConfig(msg.format(stage_name, reduction))


_numpy_ops = ["count_zero"]


class PandasAggregate(object):
    SUPPORTED: List[str] = ["sum", "prod", "max", "min", "argmax", "argmin"]

    def __init__(self, method):
        self.method = method

    def __call__(self, groups):
        return groups.agg(self.method)


class PandasNth(object):
    def __init__(self, index):
        self.index = index

    def __call__(self, groups):
        return groups.nth(self.index)


def get_pandas_reduction(stage_name, reduction):
    if not isinstance(reduction, (six.string_types, six.integer_types)):
        msg = "{}: requested reduce method is not a string or an int"
        raise BadReductionConfig(msg.format(stage_name))

    if reduction in PandasAggregate.SUPPORTED:
        return PandasAggregate(reduction)
    elif reduction in _numpy_ops:
        op = getattr(np, reduction)
        return PandasAggregate(op)
    else:
        try:
            index = int(reduction)
        except ValueError:
            pass
        else:
            return PandasNth(index)

    msg = "{}: Unknown method to reduce: '{}'"
    raise BadReductionConfig(msg.format(stage_name, reduction))
