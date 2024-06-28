import functools

from hyperscale.core_rewrite.jobs.protocols.tcp_protocol import TCPProtocol

from .hook_type import HookType


def send(target: str):
    def wraps(func):
        func.hook_type = HookType.SEND

        @functools.wraps(func)
        async def decorator(connection: TCPProtocol, *args, **kwargs):
            return await connection.send(
                target,
                await func(
                    connection,
                    *args,
                    **kwargs,
                ),
            )

        return decorator

    return wraps
