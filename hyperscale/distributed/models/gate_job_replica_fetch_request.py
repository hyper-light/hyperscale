"""Wire model ``GateJobReplicaFetchRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from dataclasses import dataclass

from .message import Message


@dataclass(slots=True)
class GateJobReplicaFetchRequest(Message):
    """Peer → peer: request cached committed replicas.

    Used by the orphan coordinator's state-repair path: when a peer
    sees a job in its leadership tracker (from an earlier
    announcement) but the local ``GateJobManager`` has no state for
    it, the peer queries other gates for the committed replica before
    declaring the job unrecoverable.

    ``leader_addr`` uses the same fetch path for the SWIM-leader repair
    case where the leader knows a gate died but does not yet know every
    job led by that gate locally.
    """

    job_id: str | None = None
    leader_addr: tuple[str, int] | None = None
