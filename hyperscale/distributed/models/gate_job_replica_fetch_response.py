"""Wire model ``GateJobReplicaFetchResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from dataclasses import dataclass, field

from .message import Message
from .gate_job_replica import GateJobReplica


@dataclass(slots=True)
class GateJobReplicaFetchResponse(Message):
    """Peer → peer: cached committed replica payload."""

    job_id: str | None = None
    leader_addr: tuple[str, int] | None = None
    replica: GateJobReplica | None = None
    replicas: list[GateJobReplica] = field(default_factory=list)
    found: bool = False
