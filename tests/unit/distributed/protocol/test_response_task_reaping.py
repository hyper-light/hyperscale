"""
The server's periodic reap of finished response tasks.

Each cycle removed finished tasks with ``deque.pop()`` -- the NEWEST task,
often one still running, which then escaped the shutdown drain while the
finished one stayed -- and awaited each finished task, so a cancelled one
raised ``CancelledError`` (not an ``Exception``) out of the loop: from then
on nothing was reaped and the deque grew for the life of the server.
Driven through the real loops on a stub server.
"""

import asyncio
from collections import deque
from types import SimpleNamespace

import pytest

from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)


class OneCycleClock:
    """Each sleep elapses at once; the second stops the server."""

    def __init__(self, server: SimpleNamespace) -> None:
        self._server = server
        self._cycles = 0

    async def wait_for(self, awaitable, timeout: float):
        self._cycles += 1
        if self._cycles > 1:
            self._server._running = False
        raise asyncio.TimeoutError()


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", ["tcp", "udp"])
async def test_reaping_keeps_running_tasks_and_survives_cancelled_ones(protocol: str) -> None:
    server = SimpleNamespace(_running=True, _cleanup_interval=0.0)
    server._clock = OneCycleClock(server)

    cancelled_task = asyncio.ensure_future(asyncio.sleep(3600))
    cancelled_task.cancel()
    finished_task = asyncio.ensure_future(asyncio.sleep(0))
    running_task = asyncio.ensure_future(asyncio.sleep(3600))
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    pending = deque([cancelled_task, finished_task, running_task])
    setattr(server, f"_pending_{protocol}_server_responses", pending)
    setattr(server, f"_{protocol}_server_sleep_task", None)

    reap = getattr(MercurySyncBaseServer, f"_cleanup_{protocol}_server_tasks")
    await reap(server)

    remaining = list(getattr(server, f"_pending_{protocol}_server_responses"))
    running_task.cancel()

    assert remaining == [running_task]
