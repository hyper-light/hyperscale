import msgspec
from typing import Callable, Awaitable

class UDPServerCall:

    def __init__(
        self,
        func: Callable[[msgspec.Struct], Awaitable[msgspec.Struct]]
    ):
        self.call = func
        self.name = func.__name__
        self.__call__ = func


def server():
    def wraps(func):
        
        def wrapper(*args, **kwrags):
            return UDPServerCall(func)
        
        wrapper.is_hook = True
        wrapper.type = 'udp'
        
        return wrapper

    return wraps
