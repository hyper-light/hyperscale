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
