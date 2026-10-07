import msgspec

from hyperscale.distributed.models import GateJobReplica


class GateJobReplicaRollback(msgspec.Struct, frozen=True, array_like=True):
    """A commit of a job's replica at ``(fence_token, sequence)`` and the
    replica it replaced (None: it replaced none) -- restored should the
    leader abort that commit."""

    fence_token: int
    sequence: int
    previous: GateJobReplica | None
