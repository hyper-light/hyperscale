import functools

from .hook_type import HookType


def send():
    def wraps(func):
        func.hook_type = HookType.SEND

        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(
                *args,
                **kwargs,
            )

        return decorator

    return wraps
