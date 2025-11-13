import msgspec
from typing import Callable, Awaitable, TypeVar, TYPE_CHECKING


if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer


T = TypeVar("T")


class TCPReceiveCall:

    def __init__(
        self,
        func: Callable[[msgspec.Struct], Awaitable[msgspec.Struct]]
    ):
        self.call = func
        self.name = func.__name__


def receive(target: str, raw: bool = False):
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
            wrapper.type = 'tcp'
            wrapper.action = 'receive'
            
            return wrapper

    return wraps