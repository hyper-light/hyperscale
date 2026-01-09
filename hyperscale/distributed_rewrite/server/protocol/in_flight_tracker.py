"""
Priority-Aware In-Flight Task Tracker (AD-32).

Provides bounded immediate execution with priority-based load shedding for
server-side incoming request handling. Ensures SWIM protocol messages
(CRITICAL priority) are never delayed or dropped.

Key Design Points:
- All operations are sync-safe (GIL-protected integer operations)
- Called from sync protocol callbacks (datagram_received, etc.)
- CRITICAL priority ALWAYS succeeds (SWIM probes/acks)
- Lower priorities shed first under load (LOW → NORMAL → HIGH)

Usage:
    tracker = InFlightTracker(limits=PriorityLimits(...))

    # In protocol callback (sync context)
    if tracker.try_acquire(MessagePriority.NORMAL):
        task = asyncio.ensure_future(handle_message(data))
        task.add_done_callback(lambda t: tracker.release(MessagePriority.NORMAL))
    else:
        # Message shed - log and drop
        pass
"""

from dataclasses import dataclass, field
from enum import IntEnum


class MessagePriority(IntEnum):
    """
    Priority levels for incoming messages.

    Priority determines load shedding order - lower priorities are shed first.
    CRITICAL messages are NEVER shed regardless of system load.
    """

    CRITICAL = 0  # SWIM probes/acks, leadership, failure detection - NEVER shed
    HIGH = 1  # Job dispatch, workflow commands, state sync
    NORMAL = 2  # Status updates, heartbeats (non-SWIM)
    LOW = 3  # Metrics, stats, telemetry, logs


@dataclass(slots=True)
class PriorityLimits:
    """
    Per-priority concurrency limits.

    A limit of 0 means unlimited. The global_limit is the sum of all
    priorities that can be in flight simultaneously.
    """

    critical: int = 0  # 0 = unlimited (SWIM must never be limited)
    high: int = 500
    normal: int = 300
    low: int = 200
    global_limit: int = 1000


@dataclass
class InFlightTracker:
    """
    Tracks in-flight tasks by priority with bounded execution.

    Thread-safety: All operations are sync-safe (GIL-protected integers).
    Called from sync protocol callbacks.

    Example:
        tracker = InFlightTracker(limits=PriorityLimits(global_limit=1000))

        def datagram_received(self, data, addr):
            priority = classify_message(data)
            if tracker.try_acquire(priority):
                task = asyncio.ensure_future(self.process(data, addr))
                task.add_done_callback(lambda t: on_done(t, priority))
            else:
                self._drop_counter.increment_load_shed()
    """

    limits: PriorityLimits = field(default_factory=PriorityLimits)

    # Per-priority counters (initialized in __post_init__)
    _counts: dict[MessagePriority, int] = field(init=False)

    # Metrics - total acquired per priority
    _acquired_total: dict[MessagePriority, int] = field(init=False)

    # Metrics - total shed per priority
    _shed_total: dict[MessagePriority, int] = field(init=False)

    def __post_init__(self) -> None:
        """Initialize counter dictionaries."""
        self._counts = {
            MessagePriority.CRITICAL: 0,
            MessagePriority.HIGH: 0,
            MessagePriority.NORMAL: 0,
            MessagePriority.LOW: 0,
        }
        self._acquired_total = {
            MessagePriority.CRITICAL: 0,
            MessagePriority.HIGH: 0,
            MessagePriority.NORMAL: 0,
            MessagePriority.LOW: 0,
        }
        self._shed_total = {
            MessagePriority.CRITICAL: 0,
            MessagePriority.HIGH: 0,
            MessagePriority.NORMAL: 0,
            MessagePriority.LOW: 0,
        }

    def try_acquire(self, priority: MessagePriority) -> bool:
        """
        Try to acquire a slot for the given priority.

        Returns True if acquired (caller should execute immediately).
        Returns False if rejected (caller should apply load shedding).

        CRITICAL priority ALWAYS succeeds - this is essential for SWIM
        protocol accuracy. If CRITICAL were ever dropped, failure detection
        would become unreliable.

        Args:
            priority: The priority level of the incoming message.

        Returns:
            True if slot acquired, False if request should be shed.
        """
        # CRITICAL never shed - SWIM protocol accuracy depends on this
        if priority == MessagePriority.CRITICAL:
            self._counts[priority] += 1
            self._acquired_total[priority] += 1
            return True

        # Check global limit first
        total_in_flight = sum(self._counts.values())
        if total_in_flight >= self.limits.global_limit:
            self._shed_total[priority] += 1
            return False

        # Check per-priority limit
        limit = self._get_limit(priority)
        if limit > 0 and self._counts[priority] >= limit:
            self._shed_total[priority] += 1
            return False

        # Slot acquired
        self._counts[priority] += 1
        self._acquired_total[priority] += 1
        return True

    def release(self, priority: MessagePriority) -> None:
        """
        Release a slot for the given priority.

        Should be called from task done callback.

        Args:
            priority: The priority level that was acquired.
        """
        if self._counts[priority] > 0:
            self._counts[priority] -= 1

    def _get_limit(self, priority: MessagePriority) -> int:
        """
        Get the limit for a given priority.

        A limit of 0 means unlimited.

        Args:
            priority: The priority level to get limit for.

        Returns:
            The concurrency limit for this priority (0 = unlimited).
        """
        if priority == MessagePriority.CRITICAL:
            return self.limits.critical
        elif priority == MessagePriority.HIGH:
            return self.limits.high
        elif priority == MessagePriority.NORMAL:
            return self.limits.normal
        else:  # LOW
            return self.limits.low

    @property
    def total_in_flight(self) -> int:
        """Total number of tasks currently in flight across all priorities."""
        return sum(self._counts.values())

    @property
    def critical_in_flight(self) -> int:
        """Number of CRITICAL priority tasks in flight."""
        return self._counts[MessagePriority.CRITICAL]

    @property
    def high_in_flight(self) -> int:
        """Number of HIGH priority tasks in flight."""
        return self._counts[MessagePriority.HIGH]

    @property
    def normal_in_flight(self) -> int:
        """Number of NORMAL priority tasks in flight."""
        return self._counts[MessagePriority.NORMAL]

    @property
    def low_in_flight(self) -> int:
        """Number of LOW priority tasks in flight."""
        return self._counts[MessagePriority.LOW]

    @property
    def total_shed(self) -> int:
        """Total number of messages shed across all priorities."""
        return sum(self._shed_total.values())

    def get_counts(self) -> dict[MessagePriority, int]:
        """Get current in-flight counts by priority."""
        return dict(self._counts)

    def get_acquired_totals(self) -> dict[MessagePriority, int]:
        """Get total acquired counts by priority."""
        return dict(self._acquired_total)

    def get_shed_totals(self) -> dict[MessagePriority, int]:
        """Get total shed counts by priority."""
        return dict(self._shed_total)

    def get_stats(self) -> dict:
        """
        Get comprehensive stats for observability.

        Returns:
            Dictionary with in_flight counts, totals, and limits.
        """
        return {
            "in_flight": {
                "critical": self._counts[MessagePriority.CRITICAL],
                "high": self._counts[MessagePriority.HIGH],
                "normal": self._counts[MessagePriority.NORMAL],
                "low": self._counts[MessagePriority.LOW],
                "total": self.total_in_flight,
            },
            "acquired_total": {
                "critical": self._acquired_total[MessagePriority.CRITICAL],
                "high": self._acquired_total[MessagePriority.HIGH],
                "normal": self._acquired_total[MessagePriority.NORMAL],
                "low": self._acquired_total[MessagePriority.LOW],
            },
            "shed_total": {
                "critical": self._shed_total[MessagePriority.CRITICAL],
                "high": self._shed_total[MessagePriority.HIGH],
                "normal": self._shed_total[MessagePriority.NORMAL],
                "low": self._shed_total[MessagePriority.LOW],
                "total": self.total_shed,
            },
            "limits": {
                "critical": self.limits.critical,
                "high": self.limits.high,
                "normal": self.limits.normal,
                "low": self.limits.low,
                "global": self.limits.global_limit,
            },
        }

    def reset_metrics(self) -> None:
        """Reset all metric counters (for testing)."""
        for priority in MessagePriority:
            self._acquired_total[priority] = 0
            self._shed_total[priority] = 0

    def __repr__(self) -> str:
        return (
            f"InFlightTracker("
            f"in_flight={self.total_in_flight}/{self.limits.global_limit}, "
            f"shed={self.total_shed})"
        )
