from typing import Any, Awaitable, Callable

from hyperscale.core.hooks.types.base.hook import Hook
from hyperscale.core.hooks.types.base.hook_type import HookType


class InternallHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        stage: str = None, 
        hook_type: HookType=HookType.INTERNAL
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            stage=stage, 
            hook_type=hook_type
        )
