import functools

from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.base.registrar import registrar

from .validator import TransformHookValidator


@registrar(HookType.TRANSFORM)
def transform(
    *names, 
    order: int=1,
    skip: bool=False
):
    
    TransformHookValidator(
        names=names,
        order=order,
        skip=skip
    )

    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper