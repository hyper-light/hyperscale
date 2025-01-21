from typing import (
    Any,
    Awaitable,
    Callable,
    List,
    Optional,
)

from hyperscale.core.engines.client.shared.timeouts import Timeouts

from .hook import Hook


def step(
    *args: str,
    timeouts: Optional[Timeouts] = None,
    tags: Optional[List[str]] = None,
):
    def wrapper(func: Callable[..., Awaitable[Any]]):
        return Hook(
            func,
            args,
            timeouts=timeouts,
            tags=tags,
        )

    return wrapper
