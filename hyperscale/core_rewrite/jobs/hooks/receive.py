import functools

from .hook_type import HookType


def receive():
    def wraps(func):
        func.hook_type = HookType.RECEIVE

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wraps
