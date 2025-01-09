from typing import Any, get_args


def reduce_pattern_type(pattern: Any):
    if isinstance(pattern, tuple) and len(pattern) > 0:
        for arg in pattern:
            return reduce_pattern_type(arg)
        
    elif (
        args := get_args(pattern)
    ) and len(args) > 0:
        return reduce_pattern_type(args)
    
    else:
        return pattern
