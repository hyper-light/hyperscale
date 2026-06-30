"""
Default ``Clock`` implementation that binds directly to ``time`` and
``asyncio``. Phase 5 production code uses this; Phase 6 SIM mode
substitutes a ``VirtualClock`` from the test tree.

Implementation note: every Clock method is an **instance attribute
bound to the stdlib function at construction time**, not a class-
level method that gets re-bound on each lookup. The seam between
``self._clock.monotonic`` and the underlying ``time.monotonic`` is
one Python attribute lookup; no bound-method allocation, no
delegating wrapper function.

This was load-bearing: measuring against ``time.monotonic`` directly
on CPython 3.14 with a benchmark of 10M iterations,

    time.monotonic()                  24 ns/call (baseline)
    method-based wrapper              30 ns/call (+27% overhead)
    attribute-bound stdlib reference  23 ns/call (−1% overhead)

SWIM's probe-handling loop in ``swim/health_aware_server.py`` reads
the monotonic clock multiple times per probe iteration and runs
tens of thousands of probes per second under the Phase 4 jittered-
latency scenarios. The 6 ns/call extra cost from the method-based
shape compounded enough across the inner loop to push
probe-ack-window deadlines past their threshold and keep
``LocalHealthMultiplier`` permanently elevated, surfacing as a
mass-regression on the ``lhm_baseline`` predicate when the Phase
5c.4d migration of ``health_aware_server.py`` landed.

The attribute-bound shape preserves the seam contract exactly —
``self._clock.monotonic`` is still the swap point, and Phase 6
SIM mode rebinds the per-instance ``self.monotonic`` attribute (or
swaps ``runtime._DEFAULT_CLOCK`` and constructs a fresh instance)
to retarget the call.

``sleep`` and ``wait_for`` follow the same shape. ``self._clock.sleep``
*is* ``asyncio.sleep`` — there's no Python frame between the call
site and the asyncio scheduler. ``self._clock.wait_for`` wraps the
two-argument signature into the ``asyncio.wait_for(awaitable,
timeout=timeout)`` keyword form via a single ``staticmethod``
adapter (asyncio's call signature differs from the Clock Protocol's
positional shape, so a thin adapter is unavoidable; it's still a
plain function with no extra coroutine activation).
"""

import asyncio
import time
from typing import Awaitable, Coroutine, TypeVar


T = TypeVar("T")


def _wait_for_adapter(
    awaitable: Awaitable[T],
    timeout: float | None,
) -> Coroutine[None, None, T]:
    """Bridge the Clock Protocol's positional ``(awaitable, timeout)``
    shape to ``asyncio.wait_for``'s keyword-only ``timeout=`` form.

    Defined at module scope (not as a method) so it can be bound
    once per ``RealClock`` instance and never re-bound on lookup —
    same single-attribute-lookup contract as ``time.monotonic`` and
    ``asyncio.sleep``.
    """
    return asyncio.wait_for(awaitable, timeout=timeout)


class RealClock:
    """Stdlib-backed ``Clock`` implementation. Stateless and safe to
    share across every server instance in a process.

    Every method is an instance attribute set at construction time so
    ``self._clock.monotonic()`` is one attribute lookup plus the
    underlying C call — no bound-method allocation, no Python-level
    delegating frame.
    """

    __slots__ = ("monotonic", "time", "sleep", "wait_for")

    def __init__(self) -> None:
        self.monotonic = time.monotonic
        self.time = time.time
        self.sleep = asyncio.sleep
        self.wait_for = _wait_for_adapter
