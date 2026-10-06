"""Wire model ``GateJobReplicaStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from enum import Enum


class GateJobReplicaStatus(Enum):
    """Per-peer acknowledgment status for a replica 2PC step."""

    PREPARED = "PREPARED"
    """Peer durably accepted the prepare; ready to commit on signal."""

    COMMITTED = "COMMITTED"
    """Peer committed the replica into its ``GateJobManager``."""

    ABORTED = "ABORTED"
    """Peer dropped a previously-prepared replica."""

    ALREADY_COMMITTED = "ALREADY_COMMITTED"
    """Peer observed a commit for ``(job_id, sequence)`` already;
    re-prepare or re-commit is a no-op acknowledgment."""

    REJECTED = "REJECTED"
    """Peer refused the request (lower sequence than already committed,
    malformed payload, or peer is shutting down)."""
