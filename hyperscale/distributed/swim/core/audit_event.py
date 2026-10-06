"""``AuditEvent`` -- pickled under the namespace
``hyperscale.distributed.swim.core.audit`` (see that module)."""

from dataclasses import dataclass
from .audit_event_record import AuditEventRecord
from .audit_event_type import AuditEventType


@dataclass(slots=True)
class AuditEvent:
    """
    A single audit event.
    
    Uses __slots__ for memory efficiency since many instances are created.
    """
    event_type: AuditEventType
    timestamp: float
    node: tuple[str, int] | None
    details: dict[str, object]
    
    def to_dict(self) -> AuditEventRecord:
        """Convert to dictionary for serialization."""
        return {
            'type': self.event_type.value,
            'timestamp': self.timestamp,
            'node': f"{self.node[0]}:{self.node[1]}" if self.node else None,
            'details': self.details,
        }
