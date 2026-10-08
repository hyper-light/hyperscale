import msgspec

from hyperscale.distributed.models import GateJobReplica

from .gate_job_replica_rollback import GateJobReplicaRollback


class GateJobReplicaDurableState(msgspec.Struct, frozen=True, array_like=True):
    """Everything a gate's two-phase commit holds for one job, as its Raft
    store keeps it (A2-G-266): the replica it acknowledged ``PREPARED`` --
    its vote, which a restart must not forget, or two gates could admit one
    idempotency key -- the replica it committed, the commits an abort may
    still roll back, and the highest sequence it sent as the job's leader,
    which no later revision of its own reuses."""

    prepared: GateJobReplica | None
    committed: GateJobReplica | None
    rollbacks: list[GateJobReplicaRollback]
    attempted_sequence: int
