"""Wire model ``GateJobReplicaAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass

from .message import Message

if TYPE_CHECKING:
    from .gate_job_replica_status import GateJobReplicaStatus


@dataclass(slots=True)
class GateJobReplicaAck(Message):
    """Peer → leader: acknowledgment for any 2PC step."""

    job_id: str
    sequence: int
    status: str
    """One of ``GateJobReplicaStatus`` values."""
    responder_id: str
    """Full node id of the peer for routing / audit trails."""
