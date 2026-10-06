"""Wire model ``GateJobReplicaCommit`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from dataclasses import dataclass

from .message import Message
from .gate_job_replica import GateJobReplica


@dataclass(slots=True)
class GateJobReplicaCommit(Message):
    """Leader → peer: promote prepared replica ``(job_id, sequence)``
    into the committed ``GateJobManager`` state.

    Carries the full replica too so a peer that missed the prepare
    (network drop, late join) can commit directly from this message.
    The peer treats this as ``prepare`` immediately followed by
    ``commit`` if it has no prepared state for ``(job_id, sequence)``.
    """

    replica: GateJobReplica
