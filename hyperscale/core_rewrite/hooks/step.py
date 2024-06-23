from typing import Any, Awaitable, Callable, Optional

from hyperscale.core_rewrite.engines.client.shared.timeouts import Timeouts

from .hook import Hook


def step(*args: str, timeouts: Optional[Timeouts] = None):
    def wrapper(func: Callable[..., Awaitable[Any]]):
        return Hook(
            func,
            args,
            timeouts=timeouts,
        )

    return wrapper
