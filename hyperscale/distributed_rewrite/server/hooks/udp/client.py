import msgspec
from typing import Callable, Awaitable, TYPE_CHECKING, TypeVar


T = TypeVar("T")


if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer



class UDPClientSendCall:

    def __init__(
        self,
        func: Callable[[msgspec.Struct], Awaitable[msgspec.Struct]]
    ):
        self.call = func
        self.name = func.__name__
        self.__call__ = func


class UDPClientHandleCall:

    def __init__(
        self,
        func: Callable[[msgspec.Struct], Awaitable[msgspec.Struct]],
    ):
        self.call = func
        self.name = func.__name__
        self.type = func.type


def send(target: str, raw: bool = False):
    encoded_target = target.encode()

    if raw:

        def wraps(func):
            
            async def wrapper(
                server: MercurySyncBaseServer,
                addr: tuple[str, int],
                data: T,
                timeout: int | float | None = None
            ):
                
                res = await func(data)

                return await server.send_bytes_udp(
                    addr,
                    encoded_target,
                    res,
                    timeout=timeout,
                )
            
            wrapper.is_hook = True
            wrapper.type = 'udp'
            wrapper.action = 'send'
            
            return wrapper

    return wraps


def handle(target: str, raw: bool = False):
    encoded_target = target.encode()

    if raw:

        def wraps(func):
            
            async def wrapper(
                server: MercurySyncBaseServer,
                addr: tuple[str, int],
                data: T,
                clock_time: int,
            ):
                
                res = await func(
                    server,
                    addr,
                    data,
                    clock_time,
                )

                return (
                    encoded_target,
                    res,
                )
            
            wrapper.is_hook = True
            wrapper.type = 'udp'
            wrapper.action = 'handle'
            
            return wrapper

    return wraps