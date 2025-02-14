import asyncio


class TerminalProtocol(asyncio.Protocol):
    async def _drain_helper(self):
        pass


def patch_transport_close(
    transport: asyncio.Transport,
    loop: asyncio.AbstractEventLoop,
):
    def close(*args, **kwargs):
        try:
            transport.close()

        except Exception:
            pass

    return close
