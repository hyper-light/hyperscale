"""``ConfirmationResult`` -- pickled under the namespace
``hyperscale.distributed.swim.roles.confirmation_manager`` (see that module)."""

from dataclasses import dataclass


@dataclass
class ConfirmationResult:
    """Result of a confirmation attempt or cleanup decision."""

    peer_id: str
    confirmed: bool
    removed: bool
    attempts_made: int
    reason: str
