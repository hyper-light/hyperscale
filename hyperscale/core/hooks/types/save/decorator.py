import functools
from typing import Tuple

from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.base.registrar import registrar

from .validator import SaveHookValidator


@registrar(HookType.SAVE)
def save(
    *names: Tuple[str, ...], save_path: str = None, order: int = 1, skip: bool = False
):
    SaveHookValidator(names=names, save_path=save_path, order=order, skip=skip)

    def wrapper(func):
        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper
