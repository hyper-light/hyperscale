from typing import Awaitable, Callable


def wrap_check(
    call: Callable[..., Awaitable[Exception | None] | Awaitable[Exception]],
):
    async def wrapped_call(*args, **kwargs):
        try:
            return await call(*args, **kwargs)

        except Exception as err:
            return err

    return wrapped_call
