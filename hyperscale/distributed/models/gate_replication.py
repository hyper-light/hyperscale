"""
Gate-tier job-state replication models (AD-31 takeover invariant).

When a gate accepts a job, every piece of state required for a peer
gate to take over leadership on the accepting gate's death must be
replicated to a quorum of peer gates *before* the client sees
``JobAck(accepted=True)``. The previous design only broadcast a
``JobLeadershipAnnouncement`` after local accept; that announcement
carried metadata only (job_id, leader_addr, fence_token,
target_dc_count) and the broadcast was async/best-effort. Peers
therefore knew *who* was leading a job but had no ``GateJobManager``
state to take over with when the leader died — orphan-coordinator
evaluation hit ``get_job(job_id) is None`` and abandoned the job
before takeover could fire.

These models give the takeover state a first-class shape and a
two-phase-commit protocol so the invariant becomes structural:
``accepted=True`` means the job is durably replicated to a quorum of
live gates and is recoverable by any of them. ``accepted=False`` means
no replication occurred and the client may retry cleanly.

Protocol summary:

  1. Leader builds a ``GateJobReplica`` capsule and sends
     ``GateJobReplicaPrepare`` to every active peer gate.
  2. Each peer applies the capsule to a *prepared* registry (separate
     from the committed ``GateJobManager`` state) and returns
     ``GateJobReplicaAck(status=PREPARED)``.
  3. The leader waits for prepare-acks summing (with self) to >=
     cluster quorum. On success it sends ``GateJobReplicaCommit`` to
     prepared peers and waits for committed acks summing (with self)
     to >= cluster quorum before the job can be accepted.
  4. On commit, a peer promotes the prepared replica into its
     ``GateJobManager`` (jobs, target_dcs, callbacks, fence tokens,
     workflow ids, submission, leadership tracker).
  5. On quorum failure the leader fires ``GateJobReplicaAbort`` at
     acked peers and rejects the client submission with
     ``error="gate_replication_quorum_unavailable"`` and a
     ``retry_after_seconds`` hint.

Idempotency: prepare/commit/abort are keyed by ``(job_id, sequence)``
where ``sequence`` is the per-job replica monotonic version (initially
``fence_token``, later bumped by deltas). Peer applies are no-ops when
the same sequence has already been observed in a non-aborted state.

Peers MUST NOT use *prepared* replicas as orphan-takeover sources —
only committed replicas count toward the takeover invariant. Prepared
state held by a peer after the leader dies mid-prepare is dropped via
explicit abort (happy path) or expires via the prepared-state TTL
(fallback). This prevents split-brain where two gates could both take
over a half-prepared job.

This module is the wire namespace of the models below. Each lives in a
file of its own and is re-homed here -- its ``__module__`` set to this
module -- so its pickled form names this module, exactly as before the
split: mixed-version clusters keep talking and data written earlier
keeps loading.
"""

from .gate_job_replica import GateJobReplica
from .gate_job_replica_abort import GateJobReplicaAbort
from .gate_job_replica_ack import GateJobReplicaAck
from .gate_job_replica_commit import GateJobReplicaCommit
from .gate_job_replica_fetch_request import GateJobReplicaFetchRequest
from .gate_job_replica_fetch_response import GateJobReplicaFetchResponse
from .gate_job_replica_prepare import GateJobReplicaPrepare
from .gate_job_replica_status import GateJobReplicaStatus

__all__ = [
    "GateJobReplica",
    "GateJobReplicaAbort",
    "GateJobReplicaAck",
    "GateJobReplicaCommit",
    "GateJobReplicaFetchRequest",
    "GateJobReplicaFetchResponse",
    "GateJobReplicaPrepare",
    "GateJobReplicaStatus",
]

_WIRE_MODELS = (
    GateJobReplicaStatus,
    GateJobReplica,
    GateJobReplicaPrepare,
    GateJobReplicaCommit,
    GateJobReplicaAbort,
    GateJobReplicaAck,
    GateJobReplicaFetchRequest,
    GateJobReplicaFetchResponse,
)

for _wire_model in _WIRE_MODELS:
    _wire_model.__module__ = __name__
