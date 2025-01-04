import asyncio


class LoggerProtocol(asyncio.Protocol):
    async def _drain_helper(self):
        pass
