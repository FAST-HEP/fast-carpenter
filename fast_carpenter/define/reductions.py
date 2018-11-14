import numpy as np
import six


__all__ = ["get_pandas_reduction"]


class BadReductionConfig(Exception):
    pass


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
        msg = "{}: requested reduce method is not a string"
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
