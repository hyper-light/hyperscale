import asyncio
import functools
from typing import Any

from hyperscale.core_rewrite.jobs.protocols.tcp_protocol import TCPProtocol

from .hook_type import HookType


def broadcast(target: str):
    def wraps(func):
        func.hook_type = HookType.BROADCAST

        @functools.wraps(func)
        async def decorator(connection: TCPProtocol, *args, **kwargs):
            response: Any = await func(
                connection,
                *args,
                **kwargs,
            )

            await asyncio.gather(
                *[
                    connection.send(
                        target,
                        response,
                        node_id=node,
                    )
                    for node in connection
                ]
            )

        return decorator

    return wraps
