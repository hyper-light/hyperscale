"""
Default ``Clock`` implementation that binds directly to ``time`` and
``asyncio``. Phase 5 production code uses this; Phase 6 SIM mode
substitutes a ``VirtualClock`` from the test tree.

Implementation note: ``sleep`` and ``wait_for`` are intentionally
**non-async** — they're plain methods that *return* the stdlib's
awaitable directly. ``await self._clock.sleep(0)`` therefore awaits
the exact same ``asyncio.sleep`` coroutine the migrated call site
would have awaited directly, with **zero extra coroutine frames** in
between.

This matters: an earlier draft defined both as ``async def`` wrappers
that did ``await asyncio.sleep(...)`` / ``await asyncio.wait_for(...)``
inside. Each such call added one extra coroutine activation and one
extra event-loop yield. For SWIM's high-frequency probe loops — which
use ``await asyncio.sleep(0)`` as a yield idiom and use
``asyncio.wait_for(ack_future, timeout=tiny)`` to wait for probe acks
under sub-second deadlines — the extra frame doubled the yield-idiom
overhead and pushed enough probe-ack waits past their deadline to
keep ``LocalHealthMultiplier`` permanently elevated. Bisecting the
Phase 5 commits down to ``swim/health_aware_server.py`` (5c.4d) and
the regex pass that replaced every ``asyncio.sleep`` / ``asyncio.wait_for``
with ``self._clock.X`` confirmed it.

The non-async shape is byte-for-byte equivalent at the call site:

    await self._clock.sleep(0)            # one ``asyncio.sleep`` await
    await self._clock.wait_for(f, 0.05)   # one ``asyncio.wait_for`` await

The ``Clock`` Protocol accepts this shape structurally — its method
signatures declare a coroutine return, and a non-async ``def`` that
returns a coroutine satisfies the same awaitable contract.
"""

import asyncio
import time
from typing import Awaitable, Coroutine, TypeVar


T = TypeVar("T")


class RealClock:
    """Stdlib-backed ``Clock`` implementation. Stateless and safe to
    share across every server instance in a process."""

    def monotonic(self) -> float:
        return time.monotonic()

    def time(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> Coroutine[None, None, None]:
        return asyncio.sleep(seconds)

    def wait_for(
        self,
        awaitable: Awaitable[T],
        timeout: float | None,
    ) -> Coroutine[None, None, T]:
        return asyncio.wait_for(awaitable, timeout=timeout)
