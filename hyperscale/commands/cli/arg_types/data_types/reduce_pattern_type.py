from typing import Any, get_args, TypeVar, get_origin


T = TypeVar('T', bound=list)


def reduce_pattern_type(
    pattern: Any, 
    stop_types: T | None = None,
):

    if stop_types and pattern in stop_types:
        return pattern

    elif isinstance(pattern, tuple) and len(pattern) > 0:
        for arg in pattern:
            return reduce_pattern_type(arg)
        
    elif (
        args := get_args(pattern)
    ) and len(args) > 0:
        return reduce_pattern_type(args)
    
    else:
        return pattern
