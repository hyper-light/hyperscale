"""
Event-driven suspicion-timer registry.

The original implementation was a Kafka-style two-level timing wheel
designed for O(1) expirations across very large numbers of entries.
For SWIM-scale clusters (tens to thousands of suspicions in flight)
the wheel's polling tick-loop is the wrong tradeoff:

* it consumes a fixed CPU budget polling for expirations even when
  nothing's expiring,
* its precision is bounded by ``fine_tick_ms``,
* (the load-bearing failure observed in this codebase) under
  event-loop saturation ``asyncio.sleep(tick_interval)`` overruns
  its target so the wheel falls behind real time. The previous
  ``_advance_fine_wheel`` advanced exactly one bucket per
  ``_tick`` call regardless of elapsed time, so a busy event loop
  silently delayed every suspicion-expiry callback by the loop-lag
  amount — long enough to push detection past the operator-budget
  envelope on small clusters with one dying peer hogging the
  probe-cycle awaits.

The fix is architectural rather than a tick-rate tweak: schedule
each entry as a direct ``loop.call_later`` against asyncio's own
heap-based timer queue. asyncio resolves the next deadline in
``O(log n)`` and fires the callback when the loop is free — no
polling, no catch-up logic, no "wheel position vs wall clock"
divergence to manage. The implementation is also dramatically
simpler: a single ``dict[NodeAddress, _Entry]`` plus per-entry
``asyncio.TimerHandle``.

The public API (``add`` / ``remove`` / ``update_expiration`` /
``get_state`` / ``contains`` / ``apply_lhm_adjustment`` /
``get_stats`` plus the sync accessors) is preserved exactly so
the ``HierarchicalFailureDetector`` is unchanged. The wheel-
specific configuration fields and the ``WheelEntry`` /
``TimingWheelBucket`` types are kept as no-op compatibility
shims for callers that still import them.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Generic, TypeVar

from hyperscale.distributed.swim.core.protocols import LoggerProtocol

from .suspicion_state import SuspicionState


# Type for node address
NodeAddress = tuple[str, int]

# Type variable for wheel entries (kept for backward-compatibility
# generics in callers that still import WheelEntry[T])
T = TypeVar("T")


@dataclass(slots=True)
class WheelEntry(Generic[T]):
    """Backward-compatibility shim.

    The previous two-level-wheel implementation used this dataclass
    to thread state + expiration time through bucket data structures.
    The event-driven replacement no longer uses it internally
    (entries are tracked by ``_Entry`` instead), but the type is
    retained so existing imports / type hints continue to resolve.
    """

    node: NodeAddress
    state: T
    expiration_time: float
    epoch: int = 0


@dataclass
class TimingWheelConfig:
    """Configuration shim for backward compatibility.

    The wheel-resolution fields (``coarse_tick_ms``, ``fine_tick_ms``,
    wheel sizes, ``fine_wheel_threshold_ms``) parameterised the
    previous polling-tick wheel. The event-driven registry does not
    consume them — asyncio's timer queue resolves expirations to its
    own precision (microseconds in practice). The fields and the
    historical ``coarse_tick_ms == fine_tick_ms * fine_wheel_size``
    invariant remain so existing callers that pass them through
    don't error out.
    """

    coarse_tick_ms: int = 1000
    coarse_wheel_size: int = 64
    fine_tick_ms: int = 100
    fine_wheel_size: int = 10
    fine_wheel_threshold_ms: int = 1000

    def __post_init__(self) -> None:
        expected_coarse = self.fine_tick_ms * self.fine_wheel_size
        if self.coarse_tick_ms != expected_coarse:
            raise ValueError(
                f"TimingWheelConfig: coarse_tick_ms must equal "
                f"fine_tick_ms * fine_wheel_size "
                f"({self.fine_tick_ms} * {self.fine_wheel_size} = "
                f"{expected_coarse}); got coarse_tick_ms={self.coarse_tick_ms}."
            )
        if self.fine_wheel_threshold_ms > expected_coarse:
            raise ValueError(
                f"TimingWheelConfig: fine_wheel_threshold_ms "
                f"({self.fine_wheel_threshold_ms}) cannot exceed the fine "
                f"wheel span ({expected_coarse})."
            )


class TimingWheelBucket:
    """Backward-compatibility shim.

    No longer used internally — entries live in ``TimingWheel._entries``
    directly. Retained because the symbol was part of the public
    detection module exports.
    """

    __slots__ = ("entries",)

    def __init__(self) -> None:
        self.entries: dict[NodeAddress, WheelEntry[SuspicionState]] = {}

    def __len__(self) -> int:
        return len(self.entries)


@dataclass(slots=True)
class _Entry:
    """Internal entry: suspicion state, expiration deadline, asyncio handle."""

    state: SuspicionState
    expiration_time: float
    timer_handle: asyncio.TimerHandle | None = None


class TimingWheel:
    """Event-driven suspicion-timer registry.

    Each entry is scheduled as an ``asyncio.TimerHandle`` via
    ``loop.call_later``. asyncio's timer-queue heap orders pending
    deadlines and fires callbacks when the loop is free — no polling
    tick loop, no wheel position to keep in sync with wall time, no
    "advance one bucket per iteration" latency cap.

    Public surface preserved from the original implementation so
    ``HierarchicalFailureDetector`` and its sync/async accessors
    work unchanged.
    """

    def __init__(
        self,
        config: TimingWheelConfig | None = None,
        on_expired: Callable[[NodeAddress, SuspicionState], None] | None = None,
        on_error: Callable[[str, Exception], None] | None = None,
        logger: LoggerProtocol | None = None,
        node_host: str = "",
        node_port: int = 0,
        node_id: str = "",
    ) -> None:
        if config is None:
            config = TimingWheelConfig()

        self._config = config
        self._on_expired = on_expired
        self._on_error = on_error
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id

        self._entries: dict[NodeAddress, _Entry] = {}
        self._running: bool = False

        # Stats
        self._entries_added: int = 0
        self._entries_removed: int = 0
        self._entries_expired: int = 0
        self._entries_moved: int = 0

    async def _log_error(self, message: str) -> None:
        if self._logger:
            from hyperscale.logging.hyperscale_logging_models import ServerError

            await self._logger.log(
                ServerError(
                    message=message,
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                )
            )

    def start(self) -> None:
        """Mark the registry as running so subsequent ``add`` calls schedule timers.

        Entries added while ``_running`` is False are tracked but not
        scheduled — they will be scheduled when ``start`` is called.
        (Symmetric with how the previous wheel held off advancing
        until ``start`` was invoked.)
        """
        if self._running:
            return
        self._running = True
        # Schedule any entries that were added pre-start.
        now = time.monotonic()
        for node, entry in self._entries.items():
            if entry.timer_handle is None:
                delay = max(0.0, entry.expiration_time - now)
                entry.timer_handle = asyncio.get_event_loop().call_later(
                    delay, self._fire_expiration, node
                )

    async def stop(self) -> None:
        """Cancel all pending timers and drop tracking state."""
        self._running = False
        for entry in self._entries.values():
            if entry.timer_handle is not None:
                entry.timer_handle.cancel()
                entry.timer_handle = None
        self._entries.clear()

    async def add(
        self,
        node: NodeAddress,
        state: SuspicionState,
        expiration_time: float,
    ) -> bool:
        """Register a suspicion. Returns False if already tracked."""
        import sys as _sys
        if node in self._entries:
            _sys.stderr.write(
                f"[WHEEL-ADD-SKIP target={node}] already tracked\n"
            )
            _sys.stderr.flush()
            return False

        entry = _Entry(state=state, expiration_time=expiration_time)
        delay = max(0.0, expiration_time - time.monotonic())
        _sys.stderr.write(
            f"[WHEEL-ADD target={node}] delay={delay:.2f} running={self._running}\n"
        )
        _sys.stderr.flush()
        if self._running:
            entry.timer_handle = asyncio.get_event_loop().call_later(
                delay, self._fire_expiration, node
            )
        self._entries[node] = entry
        self._entries_added += 1
        return True

    async def _trace_fire(self, node: NodeAddress) -> None:
        import sys as _sys
        _sys.stderr.write(f"[WHEEL-FIRE target={node}]\n")
        _sys.stderr.flush()

    async def remove(self, node: NodeAddress) -> SuspicionState | None:
        """Cancel and drop a suspicion. Returns the prior state if found."""
        entry = self._entries.pop(node, None)
        if entry is None:
            return None
        if entry.timer_handle is not None:
            entry.timer_handle.cancel()
        self._entries_removed += 1
        return entry.state

    async def update_expiration(
        self,
        node: NodeAddress,
        new_expiration_time: float,
    ) -> bool:
        """Reschedule an entry's deadline. Returns False if not tracked."""
        entry = self._entries.get(node)
        if entry is None:
            return False
        if entry.timer_handle is not None:
            entry.timer_handle.cancel()
        entry.expiration_time = new_expiration_time
        if self._running:
            delay = max(0.0, new_expiration_time - time.monotonic())
            entry.timer_handle = asyncio.get_event_loop().call_later(
                delay, self._fire_expiration, node
            )
        self._entries_moved += 1
        return True

    async def contains(self, node: NodeAddress) -> bool:
        return node in self._entries

    async def get_state(self, node: NodeAddress) -> SuspicionState | None:
        entry = self._entries.get(node)
        return entry.state if entry else None

    def _fire_expiration(self, node: NodeAddress) -> None:
        """asyncio TimerHandle callback — runs sync on the event loop.

        Pops the entry from tracking BEFORE invoking the user callback,
        symmetric with the previous wheel's ordering: the callback
        (e.g. ``HFD._handle_global_expiration``) dispatches owner-side
        validation, and any concurrent ``suspect_global`` must see that
        the timer has fired, not a still-tracked-in-the-wheel state.
        """
        import sys as _sys
        _sys.stderr.write(f"[WHEEL-FIRE target={node}]\n")
        _sys.stderr.flush()
        entry = self._entries.pop(node, None)
        if entry is None:
            # Cancelled or replaced between scheduling and firing.
            _sys.stderr.write(f"[WHEEL-FIRE-MISS target={node}]\n")
            _sys.stderr.flush()
            return
        entry.timer_handle = None
        self._entries_expired += 1
        if self._on_expired is None:
            _sys.stderr.write(f"[WHEEL-FIRE-NO-CALLBACK target={node}]\n")
            _sys.stderr.flush()
            return
        try:
            self._on_expired(node, entry.state)
        except Exception as callback_error:
            if self._on_error is not None:
                try:
                    self._on_error(
                        f"on_expired callback failed for {node}",
                        callback_error,
                    )
                except Exception:
                    pass

    async def clear(self) -> None:
        """Drop all entries (cancelling pending timers)."""
        for entry in self._entries.values():
            if entry.timer_handle is not None:
                entry.timer_handle.cancel()
                entry.timer_handle = None
        self._entries.clear()

    def get_stats(self) -> dict[str, int]:
        return {
            "entries_added": self._entries_added,
            "entries_removed": self._entries_removed,
            "entries_expired": self._entries_expired,
            "entries_moved": self._entries_moved,
            # ``cascade_count`` / wheel positions are wheel-specific
            # concepts that don't apply to the event-driven model;
            # keep the keys for backwards-compat stat dashboards.
            "cascade_count": 0,
            "current_entries": len(self._entries),
            "fine_position": 0,
            "coarse_position": 0,
        }

    async def apply_lhm_adjustment(self, multiplier: float) -> int:
        """Rescale every active timer's *remaining* duration by ``multiplier``.

        Used by ``HFD.apply_lhm_adjustment`` to extend or contract
        all in-flight suspicion deadlines when LHM changes. Each
        affected entry's ``call_later`` handle is cancelled and a
        fresh one scheduled against the rescaled deadline.

        Returns the number of entries adjusted. No-op (returns 0)
        when ``multiplier == 1.0``.
        """
        if multiplier == 1.0:
            return 0

        adjusted = 0
        now = time.monotonic()
        for node, entry in list(self._entries.items()):
            remaining = entry.expiration_time - now
            new_remaining = remaining * multiplier
            entry.expiration_time = now + new_remaining

            if entry.timer_handle is not None:
                entry.timer_handle.cancel()
            if self._running:
                entry.timer_handle = asyncio.get_event_loop().call_later(
                    max(0.0, new_remaining),
                    self._fire_expiration,
                    node,
                )
            adjusted += 1

        return adjusted

    # =========================================================================
    # Synchronous Accessors (for hot-path checks without async overhead)
    # =========================================================================

    def contains_sync(self, node: NodeAddress) -> bool:
        return node in self._entries

    def get_state_sync(self, node: NodeAddress) -> SuspicionState | None:
        entry = self._entries.get(node)
        return entry.state if entry else None
