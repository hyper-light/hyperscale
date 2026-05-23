from typing import TypeVar

from hyperscale.distributed.server.protocol.in_flight_tracker import MessagePriority

from .mock import UDPServer


T = TypeVar("T")


def receive(
    *,
    priority: MessagePriority | None = None,
    admission_group: str | None = None,
):

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
        wrapper.priority = priority
        wrapper.admission_group = admission_group
        
        return wrapper

    return wraps
