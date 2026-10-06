"""Wire model ``WorkerStateSnapshot`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .workflow_progress import WorkflowProgress


@dataclass(slots=True)
class WorkerStateSnapshot(Message):
    """
    Complete state snapshot from a worker.

    Used for state sync when a new manager becomes leader.
    """

    node_id: str  # Worker identifier
    state: str  # WorkerState value
    total_cores: int  # Total cores
    available_cores: int  # Free cores
    version: int  # State version
    # Host/port for registration reconstruction during state sync
    host: str = ""
    tcp_port: int = 0
    udp_port: int = 0
    active_workflows: dict[str, "WorkflowProgress"] = field(default_factory=dict)
