"""Wire model ``WorkerListResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.worker_state`` (see that module)."""

from dataclasses import dataclass, field

from .message import Message
from .worker_state_update import WorkerStateUpdate


@dataclass(slots=True, kw_only=True)
class WorkerListResponse(Message):
    """
    Response to list_workers request containing all locally-owned workers.

    Sent when a new manager joins the cluster and requests the worker
    list from peer managers to bootstrap its knowledge.
    """

    manager_id: str  # Responding manager's ID
    workers: list[WorkerStateUpdate] = field(default_factory=list)

    def to_bytes(self) -> bytes:
        """Serialize for transmission."""
        # Format: manager_id|worker1_bytes|worker2_bytes|...
        parts = [self.manager_id.encode()]
        parts.extend(worker.to_bytes() for worker in self.workers)
        return b"|".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkerListResponse | None":
        """Deserialize from transmission."""
        try:
            parts = data.split(b"|")
            if not parts:
                return None

            manager_id = parts[0].decode()
            workers = []

            for worker_bytes in parts[1:]:
                if worker_bytes:
                    worker_update = WorkerStateUpdate.from_bytes(worker_bytes)
                    if worker_update:
                        workers.append(worker_update)

            return cls(manager_id=manager_id, workers=workers)
        except (ValueError, UnicodeDecodeError):
            return None
