import functools
from typing import Dict, Tuple, Union

from hyperscale.core.hooks.types.base.hook_type import HookType
from hyperscale.core.hooks.types.base.registrar import registrar

from .validator import ActionHookValidator


@registrar(HookType.ACTION)
def action(
    *names: Tuple[str, ...],
    weight: int = 1,
    order: int = 1,
    metadata: Dict[str, Union[str, int]] = {},
    skip: bool = False,
):
    ActionHookValidator(
        names=names, weight=weight, order=order, metadata=metadata, skip=skip
    )

    def wrapper(func):
        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper
