"""Wire model ``GateJobReplicaPrepare`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from dataclasses import dataclass

from .message import Message
from .gate_job_replica import GateJobReplica


@dataclass(slots=True)
class GateJobReplicaPrepare(Message):
    """Leader → peer: store ``replica`` in the prepared registry."""

    replica: GateJobReplica
