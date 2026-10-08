"""``ProtocolInFlightTracker`` -- pickled under the namespace
``hyperscale.distributed.server.protocol.in_flight_tracker`` (see that module)."""

from dataclasses import dataclass, field

from .message_priority import _classify_handler_to_priority
from .message_priority import MessagePriority
from .priority_limits import PriorityLimits


@dataclass
class ProtocolInFlightTracker:
    """
    Tracks in-flight tasks by priority with bounded execution at the protocol layer.

    This tracker is designed for use in sync protocol callbacks (datagram_received,
    data_received) where asyncio.Lock cannot be used. All operations are sync-safe
    via GIL-protected integer operations.

    Note: This is distinct from higher-level application trackers. The name
    "ProtocolInFlightTracker" clarifies that this is for low-level network
    protocol message handling in MercurySyncBaseServer.

    Thread-safety: All operations are sync-safe (GIL-protected integers).
    Called from sync protocol callbacks.

    Example:
        tracker = ProtocolInFlightTracker(limits=PriorityLimits(global_limit=1000))

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
    _group_counts: dict[str, int] = field(init=False)
    _group_acquired_total: dict[str, int] = field(init=False)
    _group_shed_total: dict[str, int] = field(init=False)

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
        self._group_counts = {}
        self._group_acquired_total = {}
        self._group_shed_total = {}

    def try_acquire(
        self,
        priority: MessagePriority,
        admission_group: str | None = None,
    ) -> bool:
        """
        Try to acquire a slot for the given priority.

        Returns True if acquired (caller should execute immediately).
        Returns False if rejected (caller should apply load shedding).

        CRITICAL priority without an admission group keeps the historical
        unlimited behavior. CRITICAL traffic with a group is isolated from
        DATA/NORMAL global shedding but still bounded by the group's hard cap.

        Args:
            priority: The priority level of the incoming message.

        Returns:
            True if slot acquired, False if request should be shed.
        """
        group_limit = self._get_group_limit(admission_group)
        if group_limit > 0 and admission_group is not None:
            group_count = self._group_counts.get(admission_group, 0)
            if group_count >= group_limit:
                self._record_shed(priority, admission_group)
                return False

        # Ungrouped CRITICAL never shed - legacy control-plane behavior.
        if priority == MessagePriority.CRITICAL:
            self._record_acquired(priority, admission_group)
            return True

        # Check global limit first
        total_in_flight = sum(self._counts.values())
        if total_in_flight >= self.limits.global_limit:
            self._record_shed(priority, admission_group)
            return False

        # Check per-priority limit
        limit = self._get_limit(priority)
        if limit > 0 and self._counts[priority] >= limit:
            self._record_shed(priority, admission_group)
            return False

        # Slot acquired
        self._record_acquired(priority, admission_group)
        return True

    def release(
        self,
        priority: MessagePriority,
        admission_group: str | None = None,
    ) -> None:
        """
        Release a slot for the given priority.

        Should be called from task done callback.

        Args:
            priority: The priority level that was acquired.
        """
        if self._counts[priority] > 0:
            self._counts[priority] -= 1
        if admission_group is None:
            return

        group_count = self._group_counts.get(admission_group, 0)
        if group_count > 0:
            self._group_counts[admission_group] -= 1

    def try_acquire_for_handler(self, handler_name: str) -> bool:
        """
        Try to acquire a slot using AD-37 MessageClass classification.

        This is the preferred method for AD-37 compliant bounded execution.
        Classifies handler name to determine priority.

        Args:
            handler_name: Name of the handler (e.g., "receive_workflow_progress")

        Returns:
            True if slot acquired, False if request should be shed.
        """
        priority = _classify_handler_to_priority(handler_name)
        return self.try_acquire(priority)

    def release_for_handler(self, handler_name: str) -> None:
        """
        Release a slot using AD-37 MessageClass classification.

        Should be called from task done callback when using try_acquire_for_handler.

        Args:
            handler_name: Name of the handler that was acquired.
        """
        priority = _classify_handler_to_priority(handler_name)
        self.release(priority)

    def _record_acquired(
        self,
        priority: MessagePriority,
        admission_group: str | None,
    ) -> None:
        """Record an admitted request by priority and optional admission group."""
        self._counts[priority] += 1
        self._acquired_total[priority] += 1
        if admission_group is not None:
            self._group_counts[admission_group] = (
                self._group_counts.get(admission_group, 0) + 1
            )
            self._group_acquired_total[admission_group] = (
                self._group_acquired_total.get(admission_group, 0) + 1
            )

    def _record_shed(
        self,
        priority: MessagePriority,
        admission_group: str | None,
    ) -> None:
        """Record a shed request by priority and optional admission group."""
        self._shed_total[priority] += 1
        if admission_group is not None:
            self._group_shed_total[admission_group] = (
                self._group_shed_total.get(admission_group, 0) + 1
            )

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

    def _get_group_limit(self, admission_group: str | None) -> int:
        """Return an admission group's hard cap, or 0 for unbounded groups."""
        if admission_group == "swim":
            return self.limits.swim
        return 0

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
                "swim": self.limits.swim,
                "high": self.limits.high,
                "normal": self.limits.normal,
                "low": self.limits.low,
                "global": self.limits.global_limit,
            },
            "admission_groups": {
                "in_flight": dict(self._group_counts),
                "acquired_total": dict(self._group_acquired_total),
                "shed_total": dict(self._group_shed_total),
            },
        }

    def reset_metrics(self) -> None:
        """Reset all metric counters (for testing)."""
        for priority in MessagePriority:
            self._acquired_total[priority] = 0
            self._shed_total[priority] = 0
        self._group_acquired_total.clear()
        self._group_shed_total.clear()

    def __repr__(self) -> str:
        return (
            f"ProtocolInFlightTracker("
            f"in_flight={self.total_in_flight}/{self.limits.global_limit}, "
            f"shed={self.total_shed})"
        )
