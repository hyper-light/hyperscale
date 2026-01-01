"""
Audit trail for SWIM membership and leadership changes.

Provides a bounded event log for debugging and compliance.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from collections import deque


class AuditEventType(Enum):
    """Types of auditable events."""
    # Membership events
    NODE_JOINED = "node_joined"
    NODE_LEFT = "node_left"
    NODE_SUSPECTED = "node_suspected"
    NODE_CONFIRMED_DEAD = "node_confirmed_dead"
    NODE_REFUTED = "node_refuted"
    NODE_REJOIN = "node_rejoin"
    
    # Leadership events
    ELECTION_STARTED = "election_started"
    ELECTION_WON = "election_won"
    ELECTION_LOST = "election_lost"
    LEADER_CHANGED = "leader_changed"
    LEADER_STEPPED_DOWN = "leader_stepped_down"
    SPLIT_BRAIN_DETECTED = "split_brain_detected"
    
    # State changes
    INCARNATION_BUMPED = "incarnation_bumped"
    STATUS_CHANGED = "status_changed"


@dataclass
class AuditEvent:
    """A single audit event."""
    event_type: AuditEventType
    timestamp: float
    node: tuple[str, int] | None
    details: dict[str, Any]
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'type': self.event_type.value,
            'timestamp': self.timestamp,
            'node': f"{self.node[0]}:{self.node[1]}" if self.node else None,
            'details': self.details,
        }


@dataclass
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
    
    def __post_init__(self):
        self._events = deque(maxlen=self.max_events)
    
    def record(
        self,
        event_type: AuditEventType,
        node: tuple[str, int] | None = None,
        **details: Any,
    ) -> None:
        """Record an audit event."""
        event = AuditEvent(
            event_type=event_type,
            timestamp=time.time(),  # Wall-clock time for audit
            node=node,
            details=details,
        )
        self._events.append(event)
        self._total_events += 1
    
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
        return node_events[-count:] if len(node_events) > count else node_events
    
    def get_events_by_type(
        self,
        event_type: AuditEventType,
        count: int = 50,
    ) -> list[AuditEvent]:
        """Get recent events of a specific type."""
        type_events = [e for e in self._events if e.event_type == event_type]
        return type_events[-count:] if len(type_events) > count else type_events
    
    def get_events_since(self, since: float) -> list[AuditEvent]:
        """Get events since a timestamp."""
        return [e for e in self._events if e.timestamp >= since]
    
    def export(self) -> list[dict[str, Any]]:
        """Export all events as dictionaries."""
        return [e.to_dict() for e in self._events]
    
    def get_stats(self) -> dict[str, Any]:
        """Get audit log statistics."""
        event_counts: dict[str, int] = {}
        for event in self._events:
            key = event.event_type.value
            event_counts[key] = event_counts.get(key, 0) + 1
        
        return {
            'current_events': len(self._events),
            'max_events': self.max_events,
            'total_recorded': self._total_events,
            'events_dropped': max(0, self._total_events - self.max_events),
            'event_counts': event_counts,
        }
    
    def clear(self) -> None:
        """Clear the audit log."""
        self._events.clear()
        self._total_events = 0

