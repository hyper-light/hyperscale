"""Wire model ``ManagerLeaderInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class ManagerLeaderInfo:
    """
    Information about a manager acting as job leader (Section 9.2.1).

    Tracks manager leadership per datacenter for multi-DC deployments.
    """

    manager_addr: tuple[str, int]  # (host, port) of the manager
    fence_token: int  # Fencing token for ordering
    datacenter_id: str  # Which datacenter this manager serves
    last_updated: float  # time.monotonic() when last updated
