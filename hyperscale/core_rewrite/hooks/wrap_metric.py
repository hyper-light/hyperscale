import time
from typing import Awaitable, Callable


def wrap_metric(
    call: Callable[..., Awaitable[int | float]],
):
    async def wrapped_call(*args, **kwargs):
        return (
            await call(*args, **kwargs),
            time.monotonic(),
        )

    return wrapped_call
