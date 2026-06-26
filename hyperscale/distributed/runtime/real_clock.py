"""
Default ``Clock`` implementation that binds directly to ``time`` and
``asyncio``. Phase 5 production code uses this; Phase 6 SIM mode
substitutes a ``VirtualClock`` from the test tree.

Implementation note: each method delegates to the matching stdlib
function with no wrapper logic. The goal of Phase 5 is to introduce
the seam without changing any observable behavior or adding latency
overhead — every call here must be byte-for-byte equivalent to the
original direct call at the migrated site.
"""

import asyncio
import time
from typing import Awaitable, TypeVar


T = TypeVar("T")


class RealClock:
    """Stdlib-backed ``Clock`` implementation. Stateless and safe to
    share across every server instance in a process."""

    def monotonic(self) -> float:
        return time.monotonic()

    def time(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    async def wait_for(
        self,
        awaitable: Awaitable[T],
        timeout: float | None,
    ) -> T:
        return await asyncio.wait_for(awaitable, timeout=timeout)
