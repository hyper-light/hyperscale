"""Wire model ``WorkerStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message
from .worker_state_enum import WorkerState

if TYPE_CHECKING:
    from .manager_ping_response import ManagerPingResponse
    from .worker_heartbeat import WorkerHeartbeat
    from .worker_registration import WorkerRegistration


@dataclass(slots=True, kw_only=True)
class WorkerStatus(Message):
    """
    Status of a single worker as seen by a manager.

    Used for:
    1. Wire protocol: ManagerPingResponse reports per-worker health
    2. Internal tracking: Manager's WorkerPool tracks worker state

    The registration/heartbeat/last_seen/reserved_cores fields are
    optional and only used for internal manager tracking (not serialized
    for wire protocol responses).

    Properties provide compatibility aliases (node_id -> worker_id, health -> state).
    """

    worker_id: str  # Worker's node_id
    state: str  # WorkerState value (as string for wire)
    available_cores: int = 0  # Currently available cores
    total_cores: int = 0  # Total cores on worker
    queue_depth: int = 0  # Pending workflows
    cpu_percent: float = 0.0  # CPU utilization
    memory_percent: float = 0.0  # Memory utilization
    registration: "WorkerRegistration | None" = None
    heartbeat: "WorkerHeartbeat | None" = None
    last_seen: float = 0.0
    reserved_cores: int = 0
    is_remote: bool = False
    owner_manager_id: str = ""
    overload_state: str = "healthy"  # AD-17: healthy|busy|stressed|overloaded

    @property
    def node_id(self) -> str:
        """Alias for worker_id (internal use)."""
        return self.worker_id

    @property
    def health(self) -> WorkerState:
        """Get state as WorkerState enum (internal use)."""
        try:
            return WorkerState(self.state)
        except ValueError:
            return WorkerState.OFFLINE

    @health.setter
    def health(self, value: WorkerState) -> None:
        """Set state from WorkerState enum (internal use)."""
        object.__setattr__(self, "state", value.value)

    @property
    def short_id(self) -> str:
        """Get short form of node ID for display."""
        return self.worker_id[:12] if len(self.worker_id) > 12 else self.worker_id
