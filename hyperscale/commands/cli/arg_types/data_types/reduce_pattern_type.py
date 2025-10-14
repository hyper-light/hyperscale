import functools
import operator
from typing import Any, get_args, TypeVar


T = TypeVar("T", bound=list)


def _concat_pattern_types(pattern: Any):
    if isinstance(pattern, tuple) and len(pattern) > 0:
        return [_concat_pattern_types(arg) for arg in pattern]

    elif (args := get_args(pattern)) and len(args) > 0:
        return [_concat_pattern_types(arg) for arg in args]

    else:
        return pattern


def reduce_pattern_type(pattern: Any):
    try:
        results = _concat_pattern_types(pattern)
        return (
            functools.reduce(operator.concat, results, [])
            if len(results) > 0 and isinstance(results[0], list)
            else results
        )

    except Exception:
        pass
