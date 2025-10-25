import msgspec
from typing import Callable, Awaitable

class TCPServerCall:

    def __init__(
        self,
        func: Callable[[msgspec.Struct], Awaitable[msgspec.Struct]]
    ):
        self.call = func
        self.name = func.__name__


def server():
    def wraps(func):
        
        def wrapper(*args, **kwrags):
            return TCPServerCall(func)
        
        wrapper.is_hook = True
        wrapper.type = 'tcp'
        
        return wrapper

    return wraps