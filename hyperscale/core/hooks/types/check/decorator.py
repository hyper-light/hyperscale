import functools

from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.base.registrar import registrar

from .validator import CheckHookValidator


@registrar(HookType.CHECK)
def check(
    *names, message: str = "Did not return True.", order: int = 1, skip: bool = False
):
    CheckHookValidator(names=names, message=message, order=order, skip=skip)

    def wrapper(func):
        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper
