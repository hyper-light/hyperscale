"""
``Clock``-Protocol implementation backed by a ``SimulationLoop``'s
virtual time.

Why this is correct
-------------------

Production code reads ``await self._clock.sleep(t)`` and
``self._clock.monotonic()`` through the Phase 5 Clock seam. Under
SIM:

- ``monotonic()`` / ``time()`` → ``loop.time()`` which returns the
  loop's virtual ``_virtual_now``.
- ``sleep(t)`` delegates to ``asyncio.sleep(t)`` which internally
  calls ``loop.call_later(t, ...)``; since ``call_later`` computes
  ``when = self.time() + t``, the wake-up is registered against
  virtual time.
- ``wait_for(awaitable, timeout)`` delegates to
  ``asyncio.wait_for`` which also uses ``loop.call_later`` for its
  timeout handle.

The proof that production code reads identically across REAL and
SIM: every callsite is ``self._clock.sleep(t)`` /
``self._clock.monotonic()``; only the backing implementation
differs. The ``RealClock`` adapter zero-frames into ``time`` and
``asyncio``; the ``VirtualClock`` delegates one level deeper into
the loop. The per-call cost difference is one extra Python
attribute lookup, well below any production timing threshold.

Why not zero-overhead attribute binding
---------------------------------------

``RealClock`` (Phase 5R-5) uses ``__slots__`` and binds ``monotonic
= time.monotonic`` at construction so the call site has no Python
frame between it and the C function — necessary because some SWIM
hot paths call it tens of thousands of times per second.
``VirtualClock`` doesn't need that optimization: SIM scenarios
finish in single-digit seconds of wall time, and per-call overhead
of 50 ns isn't on any critical path. Keeping the methods regular
``def`` makes the implementation easier to read and the
``_loop.time()`` indirection obvious for callers.
"""

import asyncio

from .simulation_loop import SimulationLoop


class VirtualClock:
    """``Clock``-Protocol implementation backed by virtual time.

    Construct with the ``SimulationLoop`` that owns the virtual
    timeline. Every read of ``monotonic()`` / ``time()`` returns the
    loop's ``_virtual_now``. Every ``sleep`` / ``wait_for`` routes
    through ``asyncio.sleep`` / ``asyncio.wait_for`` which the
    custom loop schedules against virtual time.

    The bound loop reference is required at construction (no global
    lookup) so a test that constructs multiple loops in sequence
    can't accidentally cross-thread its virtual times.
    """

    def __init__(self, loop: SimulationLoop) -> None:
        self._loop = loop

    def monotonic(self) -> float:
        """Return current virtual time in seconds."""
        return self._loop.time()

    def time(self) -> float:
        """Return current virtual wall time (same as monotonic in SIM).

        SIM mode collapses ``time.monotonic`` and ``time.time``
        because there's no meaningful distinction in virtual time
        — both advance only when the loop chooses, and there's no
        wall-clock drift to model. If a future scenario needs them
        to differ (e.g., a clock-skew fault), that's a deliberate
        FaultMatrix capability addition, not a free-floating
        offset.
        """
        return self._loop.time()

    async def sleep(self, delay: float) -> None:
        """Sleep for ``delay`` virtual seconds.

        Delegates to ``asyncio.sleep``; the loop's ``call_later``
        registers the wake-up against virtual time so ``_run_once``
        will advance to it once ``_ready`` empties.
        """
        await asyncio.sleep(delay)

    async def wait_for(self, awaitable, timeout):
        """Wait for ``awaitable`` with a virtual-time timeout.

        Delegates to ``asyncio.wait_for``; same virtual-time
        registration as ``sleep``.
        """
        return await asyncio.wait_for(awaitable, timeout=timeout)
