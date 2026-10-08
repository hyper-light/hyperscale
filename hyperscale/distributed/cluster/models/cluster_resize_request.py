from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterResizeRequest(Message):
    """Add ``host:port`` to the cluster's cohort, or remove it (AD-52
    ``ResizeCluster``) -- one address per change, so any majority of the
    cohort before it and any majority after it share an address: nodes
    still configured with the old cohort and nodes configured with the new
    one can never found two clusters. A member that is not the group's
    leader passes it on once (``forwarded``)."""

    host: str
    port: int
    add: bool
    forwarded: bool = False
