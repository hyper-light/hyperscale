"""Wire model ``GateJobReplicaAbort`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from dataclasses import dataclass

from .message import Message


@dataclass(slots=True)
class GateJobReplicaAbort(Message):
    """Leader → peer: drop prepared state for the replica of version
    ``(fence_token, sequence)``.

    Fired when the leader's prepare or commit phase failed to reach
    quorum. Peers that responded ``PREPARED`` drop their prepared
    entry; a peer that already committed this version restores the
    replica it replaced. The version is exact: an abort of one
    leadership epoch's revision must not take down another epoch's at
    the same sequence.
    """

    job_id: str
    fence_token: int
    sequence: int
