"""Wire model ``StateSyncRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class StateSyncRequest(Message):
    """
    Request for state synchronization.

    Sent by new leader to gather current state.
    """

    requester_id: str  # Requesting node
    requester_role: str  # NodeRole value
    cluster_id: str = "hyperscale"  # Cluster identifier for isolation
    environment_id: str = "default"  # Environment identifier for isolation
    since_version: int = 0  # Only send updates after this version
