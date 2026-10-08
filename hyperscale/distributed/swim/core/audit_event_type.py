"""``AuditEventType`` -- pickled under the namespace
``hyperscale.distributed.swim.core.audit`` (see that module)."""

from enum import Enum


class AuditEventType(Enum):
    """Types of auditable events."""
    # Membership events
    NODE_JOINED = "node_joined"
    NODE_LEFT = "node_left"
    NODE_SUSPECTED = "node_suspected"
    NODE_CONFIRMED_DEAD = "node_confirmed_dead"
    NODE_REFUTED = "node_refuted"
    NODE_REJOIN = "node_rejoin"
    NODE_RECOVERED = "node_recovered"  # Node transitioned from DEAD back to OK
    
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
