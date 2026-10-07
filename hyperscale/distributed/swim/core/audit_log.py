"""``AuditLog`` -- pickled under the namespace
``hyperscale.distributed.swim.core.audit`` (see that module)."""

from dataclasses import dataclass, field
from collections import deque
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.logging.hyperscale_logging_models import ServerDebug

from .protocols import LoggerProtocol
from .audit_event import AuditEvent
from .audit_event_record import AuditEventRecord
from .audit_event_type import AuditEventType
from .audit_log_stats import AuditLogStats

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class AuditLog:
    """
    Bounded audit log for membership and leadership events.
    
    Maintains a fixed-size circular buffer of events.
    Useful for debugging, compliance, and post-mortem analysis.
    """
    max_events: int = 1000
    """Maximum number of events to retain."""
    
    _events: deque[AuditEvent] = field(default_factory=deque)
    _total_events: int = 0
    _events_dropped: int = 0  # Track dropped events for monitoring
    
    # Maximum counter value to prevent overflow
    MAX_COUNTER_VALUE: int = 2**31 - 1
    
    # Logger for structured logging (optional)
    _logger: LoggerProtocol | None = None
    _node_host: str = ""
    _node_port: int = 0
    _node_id: int = 0
    
    def __post_init__(self):
        self._events = deque(maxlen=self.max_events)
    
    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: int,
    ) -> None:
        """Set logger for structured logging."""
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id
    
    def record(
        self,
        event_type: AuditEventType,
        node: tuple[str, int] | None = None,
        **details: object,
    ) -> None:
        """Record an audit event."""
        # Track if we're about to drop an event
        at_capacity = len(self._events) >= self.max_events
        
        event = AuditEvent(
            event_type=event_type,
            timestamp=_DEFAULT_CLOCK.time(),  # Wall-clock time for audit
            node=node,
            details=details,
        )
        self._events.append(event)

        self._count_total_event()

        self._count_dropped_event(at_capacity)

    def _count_total_event(self) -> None:
        """Count a recorded event, saturating at ``MAX_COUNTER_VALUE``."""
        if self._total_events < self.MAX_COUNTER_VALUE:
            self._total_events += 1

    def _count_dropped_event(self, at_capacity: bool) -> None:
        """Count an event the full deque pushed out, saturating at ``MAX_COUNTER_VALUE``."""
        if at_capacity and self._events_dropped < self.MAX_COUNTER_VALUE:
            self._events_dropped += 1
    
    def get_recent(self, count: int = 100) -> list[AuditEvent]:
        """Get the most recent events."""
        events = list(self._events)
        return events[-count:] if len(events) > count else events
    
    def get_events_for_node(
        self,
        node: tuple[str, int],
        count: int = 50,
    ) -> list[AuditEvent]:
        """Get recent events for a specific node."""
        node_events = [e for e in self._events if e.node == node]
        # The slice is the whole list whenever it holds no more than ``count``.
        return node_events[-count:]
    
    def get_events_by_type(
        self,
        event_type: AuditEventType,
        count: int = 50,
    ) -> list[AuditEvent]:
        """Get recent events of a specific type."""
        type_events = [e for e in self._events if e.event_type == event_type]
        # The slice is the whole list whenever it holds no more than ``count``.
        return type_events[-count:]
    
    def get_events_since(self, since: float) -> list[AuditEvent]:
        """Get events since a timestamp."""
        return [e for e in self._events if e.timestamp >= since]
    
    def export(self) -> list[AuditEventRecord]:
        """Export all events as dictionaries."""
        return [e.to_dict() for e in self._events]
    
    def get_stats(self) -> AuditLogStats:
        """Get audit log statistics."""
        event_counts: dict[str, int] = {}
        for event in self._events:
            key = event.event_type.value
            event_counts[key] = event_counts.get(key, 0) + 1
        
        return {
            'current_events': len(self._events),
            'max_events': self.max_events,
            'total_recorded': self._total_events,
            'events_dropped': self._events_dropped,
            'event_counts': event_counts,
        }
    
    async def log_capacity_warning(self) -> bool:
        """
        Log a warning if events are being dropped due to capacity.
        
        Returns True if a warning was logged.
        Should be called periodically (e.g., from cleanup loop).
        """
        if self._capacity_warning_suppressed():
            return False

        try:
            await self._logger.log(ServerDebug(
                message=f"[AuditLog] Events dropped due to capacity: {self._events_dropped} "
                        f"(max_events={self.max_events}, total_recorded={self._total_events})",
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            ))
            return True
        except Exception:
            return False
    
    def _capacity_warning_suppressed(self) -> bool:
        """True when there is no logger or no event has been dropped."""
        return not self._logger or self._events_dropped == 0

    def clear(self) -> None:
        """Clear the audit log."""
        self._events.clear()
        self._total_events = 0
        self._events_dropped = 0
