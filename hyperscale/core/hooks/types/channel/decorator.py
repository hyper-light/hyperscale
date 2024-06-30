import functools
from typing import Tuple

from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.base.registrar import registrar

from .validator import ChannelHookValidator


@registrar(HookType.CHANNEL)
def channel(*names: Tuple[str, ...], order: int = 1, skip: bool = False):
    ChannelHookValidator(names=names, order=order, skip=skip)

    def wrapper(func) -> Hook:
        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper
