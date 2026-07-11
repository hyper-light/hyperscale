"""
Clock interface — the dependency-injection seam for every wall- and
monotonic-time read, every ``asyncio.sleep``, and every
``asyncio.wait_for`` in the distributed runtime.

Why a Protocol and not an ABC: the codebase already uses structural
``Callable[[], float]`` time sources at
``hyperscale/distributed/health/extension_decision.py`` and
``hyperscale/distributed/nodes/worker/extension_trigger.py``. Phase 5
generalizes that precedent into a single object so each consuming
class takes one ``clock`` parameter instead of four optional callable
parameters (``time_source``, ``sleep``, ``wait_for``, etc.). ``Protocol``
keeps the structural typing while letting Phase 6's stateful
``VirtualClock`` live in ``tests/`` without crossing the production
import boundary or inheriting from a production base class.

What's NOT on the Protocol:

* ``loop.call_later`` / ``loop.call_at`` — not used anywhere in
  ``hyperscale/distributed/`` today. Phase 6's deterministic runner can
  synthesize them from ``sleep`` if needed.
* ``perf_counter`` — used for measurement, not control-flow scheduling.
  The few production sites that need it can keep raw ``time.perf_counter``;
  it doesn't affect SIM correctness.
* Logical clocks — ``LamportClock`` / ``HybridLamportClock`` track
  causality, not wall time, and stay on their existing APIs.
"""

from typing import Awaitable, Protocol, TypeVar


T = TypeVar("T")


class Clock(Protocol):
    """Read wall/monotonic time and schedule asynchronous waits.

    Every production consumer routes through this interface; the
    ``RealClock`` implementation binds to the stdlib functions and
    Phase 6's ``VirtualClock`` will model a simulated timeline.

    Methods kept minimal — Phase 5's exit criterion is that no
    production module under ``hyperscale/distributed/`` calls
    ``time.monotonic`` / ``time.time`` / ``time.monotonic_ns`` /
    ``asyncio.sleep`` / ``asyncio.wait_for`` directly, so the Protocol
    covers exactly those operations and nothing more. ``monotonic_ns``
    joined the original four once id-generation sites were found reading
    ``time.monotonic_ns`` directly for token uniqueness — the same
    determinism seam applies.
    """

    def monotonic(self) -> float:
        """Return monotonically-increasing seconds (the
        ``time.monotonic`` equivalent). Use for elapsed-time / timeout
        bookkeeping. Never use for absolute wall time."""
        ...

    def monotonic_ns(self) -> int:
        """Return monotonically-increasing nanoseconds (the
        ``time.monotonic_ns`` equivalent).

        Added because id-generation sites (SWIM probe request tokens,
        gate rejoin tokens) embed a high-resolution monotonic timestamp
        for uniqueness, and ``time.monotonic_ns`` is as much a
        SIM-swappable time read as ``monotonic`` — leaving it un-seamed
        left those ids non-deterministic under replay. Integer
        nanoseconds, not float seconds, so the low-order digits that
        give the token its uniqueness survive."""
        ...

    def time(self) -> float:
        """Return wall-clock seconds since the UNIX epoch (the
        ``time.time`` equivalent). Reserved for the few sites that
        genuinely need wall time — Snowflake ID generation, log
        timestamping. Most timing should use ``monotonic``."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Yield to the event loop for ``seconds``. Mirrors
        ``asyncio.sleep`` semantics: ``0`` yields without blocking."""
        ...

    async def wait_for(
        self,
        awaitable: Awaitable[T],
        timeout: float | None,
    ) -> T:
        """Await ``awaitable`` with a deadline. Mirrors
        ``asyncio.wait_for`` semantics: ``None`` disables the timeout.

        The timeout belongs on the clock — not the bare awaitable —
        because Phase 6's ``VirtualClock`` advances simulated time
        independently of the real event loop; the awaitable runs
        normally while the deadline ticks against virtual time.
        """
        ...
