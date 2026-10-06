"""``UnconfirmedPeerState`` -- pickled under the namespace
``hyperscale.distributed.swim.roles.confirmation_manager`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models.distributed import NodeRole


@dataclass(slots=True)
class UnconfirmedPeerState:
    """State tracking for an unconfirmed peer."""

    peer_id: str
    peer_address: tuple[str, int]
    role: NodeRole
    discovered_at: float
    confirmation_attempts_made: int = 0
    next_attempt_at: float | None = None
    last_attempt_at: float | None = None
