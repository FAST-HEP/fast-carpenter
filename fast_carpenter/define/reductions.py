import numpy as np
import six


__all__ = ["get_pandas_reduction"]


class BadReductionConfig(Exception):
    pass


class JaggedNth():
    def __init__(self, index, fill_missing):
        self.index = index
        self.fill_missing = fill_missing

    def __call__(self, array):
        mask = array.counts > self.index
        output = np.full(len(array), self.fill_missing)
        output[mask] = array[mask, self.index]
        return output


class JaggedMethod():
    def __init__(self, method):
        self.method_name = method

    def __call__(self, array):
        return getattr(array, self.method_name)()


class JaggedProperty():
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


class PandasAggregate():
    def __init__(self, method):
        self.method = method

    def __call__(self, groups):
        return groups.agg(self.method)


class PandasNth():
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
