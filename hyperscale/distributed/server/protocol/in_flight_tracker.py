"""
Priority-Aware In-Flight Task Tracker (AD-32, AD-37).

Provides bounded immediate execution with priority-based load shedding for
server-side incoming request handling. Ensures SWIM protocol messages
(CRITICAL priority) are never delayed or dropped.

Key Design Points:
- All operations are sync-safe (GIL-protected integer operations)
- Called from sync protocol callbacks (datagram_received, etc.)
- CRITICAL priority ALWAYS succeeds (SWIM probes/acks)
- Lower priorities shed first under load (LOW → NORMAL → HIGH)

AD-37 Integration:
- MessagePriority maps directly to AD-37 MessageClass via MESSAGE_CLASS_TO_PRIORITY
- CONTROL (MessageClass) → CRITICAL (MessagePriority) - never shed
- DISPATCH → HIGH - shed under overload
- DATA → NORMAL - explicit backpressure
- TELEMETRY → LOW - shed first

Usage:
    tracker = InFlightTracker(limits=PriorityLimits(...))

    # In protocol callback (sync context) - direct priority
    if tracker.try_acquire(MessagePriority.NORMAL):
        task = asyncio.ensure_future(handle_message(data))
        task.add_done_callback(lambda t: tracker.release(MessagePriority.NORMAL))
    else:
        # Message shed - log and drop
        pass

    # AD-37 compliant usage - handler name classification
    if tracker.try_acquire_for_handler("receive_workflow_progress"):
        task = asyncio.ensure_future(handle_message(data))
        task.add_done_callback(lambda t: tracker.release_for_handler("receive_workflow_progress"))
"""

from dataclasses import dataclass, field
from enum import IntEnum


# AD-37 Handler classification sets (duplicated from message_class.py to avoid circular import)
# message_class.py imports MessagePriority from this module, so we can't import back
_CONTROL_HANDLERS: frozenset[str] = frozenset({
    # SWIM protocol
    "ping", "ping_req", "ack", "nack", "indirect_ping", "indirect_ack",
    # Cancellation (AD-20)
    "cancel_workflow", "cancel_job", "workflow_cancelled", "job_cancellation_complete",
    # Leadership transfer
    "leadership_transfer", "job_leader_transfer", "receive_job_leader_transfer", "job_leader_worker_transfer",
    # Failure detection
    "suspect", "alive", "dead", "leave",
})

_DISPATCH_HANDLERS: frozenset[str] = frozenset({
    # Job dispatch
    "submit_job", "receive_submit_job", "dispatch_workflow", "receive_workflow_dispatch",
    # State sync
    "state_sync_request", "state_sync_response", "request_state_sync",
    # Registration
    "worker_register", "receive_worker_register", "manager_register", "receive_manager_register",
    # Workflow commands
    "workflow_dispatch_ack", "workflow_final_result",
})

_DATA_HANDLERS: frozenset[str] = frozenset({
    # Progress updates
    "workflow_progress", "receive_workflow_progress", "workflow_progress_ack",
    # Stats updates
    "receive_stats_update", "send_stats_update",
    # AD-34 timeout coordination
    "receive_job_progress_report", "receive_job_timeout_report", "receive_job_global_timeout", "receive_job_final_status",
    # Heartbeats (non-SWIM)
    "heartbeat", "manager_heartbeat", "worker_heartbeat",
    # Job progress (gate handlers)
    "receive_job_progress",
})

_TELEMETRY_HANDLERS: frozenset[str] = frozenset({
    # Metrics
    "metrics_report", "debug_stats", "trace_event",
    # Health probes (non-critical)
    "health_check", "readiness_check", "liveness_check",
    # Federated health (best-effort)
    "xprobe", "xack",
})


class MessagePriority(IntEnum):
    """
    Priority levels for incoming messages.

    Priority determines load shedding order - lower priorities are shed first.
    CRITICAL messages are NEVER shed regardless of system load.

    Maps to AD-37 MessageClass:
    - CRITICAL ← CONTROL (SWIM, cancellation, leadership)
    - HIGH ← DISPATCH (job submission, workflow dispatch)
    - NORMAL ← DATA (progress updates, stats)
    - LOW ← TELEMETRY (metrics, debug)
    """

    CRITICAL = 0  # SWIM probes/acks, leadership, failure detection - NEVER shed
    HIGH = 1  # Job dispatch, workflow commands, state sync
    NORMAL = 2  # Status updates, heartbeats (non-SWIM)
    LOW = 3  # Metrics, stats, telemetry, logs


def _classify_handler_to_priority(handler_name: str) -> MessagePriority:
    """
    Classify a handler name to MessagePriority using AD-37 classification.

    This is a module-internal function that duplicates the logic from
    message_class.py to avoid circular imports.

    Args:
        handler_name: Name of the handler (e.g., "receive_workflow_progress")

    Returns:
        MessagePriority for the handler
    """
    if handler_name in _CONTROL_HANDLERS:
        return MessagePriority.CRITICAL
    if handler_name in _DISPATCH_HANDLERS:
        return MessagePriority.HIGH
    if handler_name in _DATA_HANDLERS:
        return MessagePriority.NORMAL
    if handler_name in _TELEMETRY_HANDLERS:
        return MessagePriority.LOW
    # Default to NORMAL for unknown handlers (conservative)
    return MessagePriority.NORMAL


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
