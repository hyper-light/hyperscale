"""Wire model ``WorkerStatePiggybackUpdate`` -- pickled under the wire namespace
``hyperscale.distributed.models.worker_state`` (see that module)."""

from dataclasses import dataclass

from .worker_state_update import WorkerStateUpdate


@dataclass(slots=True, kw_only=True)
class WorkerStatePiggybackUpdate:
    """
    A worker state update to be piggybacked on SWIM messages.

    Similar to PiggybackUpdate but for worker state dissemination.
    Uses __slots__ for memory efficiency since many instances are created.
    """

    update: WorkerStateUpdate
    timestamp: float
    broadcast_count: int = 0
    max_broadcasts: int = 10

    def should_broadcast(self) -> bool:
        """Check if this update should still be piggybacked."""
        return self.broadcast_count < self.max_broadcasts

    def mark_broadcast(self) -> None:
        """Mark that this update was piggybacked."""
        self.broadcast_count += 1
