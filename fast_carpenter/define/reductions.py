import numpy as np
import six
from ..expressions import deconstruct_jaggedness, reconstruct_jaggedness


__all__ = ["get_pandas_reduction"]


class BadReductionConfig(Exception):
    pass


class JaggedNth(object):
    def __init__(self, index, fill_missing, force_float=True):
        self.index = index
        self.fill_missing = fill_missing
        self.dtype = None
        if fill_missing is True or fill_missing is False:
            self.dtype = bool
        elif force_float or isinstance(fill_missing, float):
            self.dtype = float
        else:
            self.dtype = int

    def __call__(self, array):
        # The next two lines ought to be enough
        # result = array.pad(abs(self.index) + int(self.index >= 0))
        # result = result[..., self.index]

        # Flatten out the first K-1 dimensions:
        flat, counts = deconstruct_jaggedness(array, [])
        result = reconstruct_jaggedness(flat, counts[:1])

        # Now get the Nth item on the last dimension
        result = result.pad(abs(self.index) + int(self.index >= 0))
        result = result[..., self.index]

        # Now replay the remaining dimensions on this
        result = reconstruct_jaggedness(result, counts[1:])

        if self.dtype is not None:
            result = result.astype(self.dtype)
        result = result.fillna(self.fill_missing)
        return result


class JaggedMethod(object):
    def __init__(self, method):
        self.method_name = method

    def __call__(self, array):
        return getattr(array, self.method_name)()


class JaggedProperty(object):
    def __init__(self, prop_name):
        self.prop_name = prop_name

    def __call__(self, array):
        return getattr(array, self.prop_name)


_jagged_methods = ["sum", "prod", "any", "all", "count_nonzero",
                   "max", "min", "argmin", "argmax"]
_jagged_properties = ["counts"]


def get_awkward_reduction(stage_name, reduction, fill_missing=np.nan):
    if isinstance(reduction, six.integer_types):
        return JaggedNth(int(reduction), fill_missing)

    if not isinstance(reduction, six.string_types):
        msg = "{}: requested reduce method is not a string or an int"
        raise BadReductionConfig(msg.format(stage_name))

    if reduction in _jagged_methods:
        return JaggedMethod(reduction)
    if reduction in _jagged_properties:
        return JaggedProperty(reduction)

    msg = "{}: Unknown method to reduce: '{}'"
    raise BadReductionConfig(msg.format(stage_name, reduction))


_pandas_aggregates = ["sum", "prod", "max", "min", "argmax", "argmin"]
_numpy_ops = ["count_zero"]


class PandasAggregate(object):
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

    if reduction in _pandas_aggregates:
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
