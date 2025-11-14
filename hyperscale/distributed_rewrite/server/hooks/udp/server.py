from typing import TypeVar
from .mock import UDPServer


T = TypeVar("T")


def receive():

    def wraps(func):
        
        async def wrapper(
            server: UDPServer,
            addr: tuple[str, int],
            data: T,
            clock_time: int,
        ):
            
            return await func(
                server,
                addr,
                data,
                clock_time,
            )
        
        wrapper.is_hook = True
        wrapper.type = 'udp'
        wrapper.action = 'receive'
        wrapper.name = func.__name__
        
        return wrapper

    return wraps