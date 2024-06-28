import functools

from hyperscale.core_rewrite.jobs.protocols.tcp_protocol import TCPProtocol

from .hook_type import HookType


def push(target: str):
    def wraps(func):
        func.hook_type = HookType.PUSH

        @functools.wraps(func)
        async def decorator(connection: TCPProtocol, *args, **kwargs):
            node_id, response = await func(
                connection,
                *args,
                **kwargs,
            )
            return await connection.send(
                target,
                response,
                node_id=node_id,
            )

        return decorator

    return wraps
