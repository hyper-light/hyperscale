from typing import TypeVar
from .mock import TCPServer


T = TypeVar("T")



def send(target: str):

    def wraps(func):
        
        async def wrapper(
            server: TCPServer,
            addr: tuple[str, int],
            data: T,
            timeout: int | float | None = None
        ):
            
            (
                addr,
                data,
                timeout,
            ) = await func(
                server,
                addr,
                data,
                timeout=timeout,
            )

            return await server.send_tcp(
                addr,
                target,
                data,
                timeout=timeout,
            )
    

        wrapper.is_hook = True
        wrapper.type = 'tcp'
        wrapper.action = 'send'
        wrapper.name = func.__name__
        
        return wrapper

    return wraps


def handle(target: str):
    encoded_target = target.encode()

    def wraps(func):
        
        async def wrapper(
            server: TCPServer,
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
        wrapper.type = 'tcp'
        wrapper.action = 'handle'
        wrapper.name = func.__name__
        wrapper.target = encoded_target
        
        return wrapper

    return wraps