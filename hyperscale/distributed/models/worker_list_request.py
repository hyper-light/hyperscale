"""Wire model ``WorkerListRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.worker_state`` (see that module)."""

from dataclasses import dataclass

from .message import Message


@dataclass(slots=True, kw_only=True)
class WorkerListRequest(Message):
    """
    Request for worker list from peer managers.

    Sent when a manager joins the cluster to bootstrap knowledge
    of workers registered with other managers.
    """

    requester_id: str  # Requesting manager's ID
    requester_datacenter: str = ""  # Requester's datacenter
