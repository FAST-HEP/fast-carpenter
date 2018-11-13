import numpy as np
import six


__all__ = ["get_pandas_reduction"]


class BadReductionConfig(Exception):
    pass


_pandas_aggregates = ["sum", "max", "min", "argmax", "argmin"]
_numpy_ops = ["count_zero"]


def get_pandas_reduction(stage_name, reduction):
    if not isinstance(reduction, (six.string_types, six.integer_types)):
        msg = "{}: requested reduce method is not a string"
        raise BadReductionConfig(msg.format(stage_name))

    if reduction in _pandas_aggregates:
        return lambda groups: groups.agg(reduction)
    elif reduction in _numpy_ops:
        op = getattr(np, reduction)
        return lambda groups: groups.agg(op)
    else:
        try:
            index = int(reduction)
        except ValueError:
            pass
        else:
            return lambda groups: groups.nth(index)

    msg = "{}: Unknown method to reduce: '{}'"
    raise BadReductionConfig(msg.format(stage_name, reduction))
