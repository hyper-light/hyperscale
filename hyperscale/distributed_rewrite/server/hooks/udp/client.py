import msgspec
from typing import Callable, Awaitable


class UDPClientCall:

    def __init__(
        self,
        func: Callable[[msgspec.Struct], Awaitable[msgspec.Struct]]
    ):
        self.call = func
        self.name = func.__name__
        self.__call__ = func


def client():
    def wraps(func):
        
        def wrap_client(*args, **kwrags):
            return UDPClientCall(func)
        
        wrap_client.is_hook = True
        wrap_client.type = 'udp'
        
        return wrap_client

    return wraps
