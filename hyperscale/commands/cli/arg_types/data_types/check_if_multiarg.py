import functools
import operator
from typing import Any, get_origin, get_args


def _check_if_multiarg(arg: Any, complex_types: list[type[Any]]):
    if arg in [list, set] or get_origin(arg) in [list, set]:
        return [True]
    
    if arg in complex_types:
        return [False]
    
    if isinstance(arg, tuple) and len(arg) > 0:
        return [_check_if_multiarg(subarg, complex_types) for subarg in arg]
    
    elif (args := get_args(arg)) and len(args) > 0:
        return [
            _check_if_multiarg(subarg, complex_types) for subarg in args
        ]
    
    else:
        return [False]
    

def check_if_multiarg(arg: Any, complex_types: list[type[Any]]):
    try:
        results = _check_if_multiarg(arg, complex_types)
        return True in ( 
            functools.reduce(operator.concat, results, [])
            if len(results) > 0 and isinstance(results[0], list)
            else results
        )

    except Exception:
        pass
