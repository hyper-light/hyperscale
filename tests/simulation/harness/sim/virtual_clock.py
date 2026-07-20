"""
``Clock``-Protocol implementation backed by a ``SimulationLoop``'s
virtual time.

Why this is correct
-------------------

Production code reads ``await self._clock.sleep(t)`` and
``self._clock.monotonic()`` through the Phase 5 Clock seam. Under
SIM:

- ``monotonic()`` / ``time()`` → ``loop.time()`` which returns the
  loop's virtual ``_virtual_now`` (``time()`` adds the wall-skew
  offset when a scenario arms one via ``set_wall_offset`` — the
  D1/D2 clock-fault knob; zero by default).
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
import math

from .simulation_loop import SimulationLoop


class VirtualClock:
    """``Clock``-Protocol implementation backed by virtual time.

    Construct with the ``SimulationLoop`` that owns the virtual
    timeline. Every read of ``monotonic()`` / ``time()`` returns the
    loop's ``_virtual_now`` (``time()`` plus the armed wall offset —
    zero by default, see ``set_wall_offset``). Every ``sleep`` /
    ``wait_for`` routes through ``asyncio.sleep`` /
    ``asyncio.wait_for`` which the custom loop schedules against
    virtual time.

    The bound loop reference is required at construction (no global
    lookup) so a test that constructs multiple loops in sequence
    can't accidentally cross-thread its virtual times.
    """

    def __init__(self, loop: SimulationLoop) -> None:
        self._loop = loop
        self._wall_offset = 0.0

    def monotonic(self) -> float:
        """Return current virtual time in seconds."""
        return self._loop.time()

    def monotonic_ns(self) -> int:
        """Return current virtual time in integer nanoseconds.

        Derived from the same virtual timeline as ``monotonic`` so
        id-generation sites that embed a nanosecond timestamp are
        deterministic under replay. Two reads at the same virtual
        instant return the same value — uniqueness of those ids comes
        from their seeded random component, not from this clock.
        """
        return int(self._loop.time() * 1_000_000_000)

    def time(self) -> float:
        """Return current virtual wall time.

        By default (offset zero) SIM collapses ``time.monotonic`` and
        ``time.time`` — both advance only when the loop chooses, and
        there is no drift to model. The deliberate exception is the
        clock-skew fault: ``set_wall_offset`` shifts every subsequent
        WALL read by the armed delta while ``monotonic()`` and every
        timer stay on the loop's virtual timeline — exactly how a real
        NTP step moves ``time.time`` but never ``CLOCK_MONOTONIC``.
        """
        return self._loop.time() + self._wall_offset

    def set_wall_offset(self, delta_seconds: float) -> None:
        """Skew this node's WALL clock by ``delta_seconds``.

        Subsequent ``time()`` reads return ``loop.time() +
        delta_seconds``; ``monotonic()`` / ``monotonic_ns()`` /
        ``sleep`` / ``wait_for`` and every loop timer are UNTOUCHED —
        the fault models an NTP step, and real steps move the wall
        clock without ever moving ``CLOCK_MONOTONIC`` or armed timer
        deadlines. Negative deltas (a backwards wall step) and mid-run
        re-sets (repeated jumps) are both legal — both are things NTP
        does. Lockstep coherence is unaffected: the loop's virtual
        time stays global across processes; only this process's wall
        READS shift, which is what makes the skew per-node.

        The offset is absolute, not cumulative: arming ``+5`` then
        ``-3`` leaves the wall clock 3 seconds BEHIND the virtual
        timeline, not 2 ahead — each call models one step to a
        definite skew, so a scenario's schedule reads as data.
        """
        if not math.isfinite(delta_seconds):
            raise ValueError(
                f"wall offset must be finite (got {delta_seconds!r}) — "
                "a non-finite offset would poison every subsequent "
                "wall read"
            )
        self._wall_offset = delta_seconds

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
