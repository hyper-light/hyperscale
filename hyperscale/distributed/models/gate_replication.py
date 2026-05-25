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
     cluster quorum. On success it commits locally, then fires
     ``GateJobReplicaCommit`` at acked peers (fire-and-forget after
     quorum — the protocol invariant is already satisfied).
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
"""

from dataclasses import dataclass, field
from enum import Enum

from .message import Message


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


@dataclass(slots=True)
class GateJobReplica(Message):
    """The takeover capsule.

    Carries everything a peer gate needs to assume leadership of a job
    when the accepting gate dies. Designed for idempotent apply: peers
    track ``(job_id, sequence)`` and ignore re-deliveries.

    Fields map directly into ``GateJobManager`` / ``GateRuntimeState``
    state on the peer:

    * ``status_seed``, ``submitted_at`` → ``GlobalJobStatus`` written
      into ``_jobs[job_id]``.
    * ``target_dcs`` → ``_job_target_dcs[job_id]``.
    * ``callback_addr`` → ``_job_callbacks[job_id]`` and
      ``_progress_callbacks[job_id]``.
    * ``fence_token`` → ``_job_fence_tokens[job_id]`` and the
      leadership-tracker fencing token via
      ``record_external_leader``.
    * ``workflow_ids`` → ``_job_workflow_ids[job_id]``.
    * ``submission_payload`` → ``_job_submissions[job_id]`` (raw
      serialized ``JobSubmission`` so peers do not need to deserialize
      the submission unless they actually take over and dispatch).
    * ``leader_id`` + ``leader_addr`` →
      ``_job_leadership_tracker.record_external_leader``.
    * ``origin_gate_addr`` is the gate that accepted the client
      submission (== ``leader_addr`` at submission time, but tracked
      separately so it survives later leader changes).
    """

    job_id: str
    sequence: int
    fence_token: int
    leader_id: str
    leader_addr: tuple[str, int]
    origin_gate_addr: tuple[str, int]
    callback_addr: tuple[str, int] | None
    target_dcs: list[str]
    target_dc_count: int
    status_seed: str
    submitted_at: float
    workflow_ids: list[str] = field(default_factory=list)
    submission_payload: bytes = b""


@dataclass(slots=True)
class GateJobReplicaPrepare(Message):
    """Leader → peer: store ``replica`` in the prepared registry."""

    replica: GateJobReplica


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


@dataclass(slots=True)
class GateJobReplicaAbort(Message):
    """Leader → peer: drop prepared state for ``(job_id, sequence)``.

    Fired when the leader's prepare phase failed to reach quorum and
    the leader has rejected the client submission. Peers that
    previously responded ``PREPARED`` MUST drop their prepared entry
    on receipt. No-op when the peer has already committed (a separate
    leader-level coordination problem that should not occur in a
    correctly-running cluster).
    """

    job_id: str
    sequence: int


@dataclass(slots=True)
class GateJobReplicaAck(Message):
    """Peer → leader: acknowledgment for any 2PC step."""

    job_id: str
    sequence: int
    status: str
    """One of ``GateJobReplicaStatus`` values."""
    responder_id: str
    """Full node id of the peer for routing / audit trails."""


@dataclass(slots=True)
class GateJobReplicaFetchRequest(Message):
    """Peer → peer: request a cached committed replica for ``job_id``.

    Used by the orphan coordinator's state-repair path: when a peer
    sees a job in its leadership tracker (from an earlier
    announcement) but the local ``GateJobManager`` has no state for
    it, the peer queries other gates for the committed replica before
    declaring the job unrecoverable.
    """

    job_id: str


@dataclass(slots=True)
class GateJobReplicaFetchResponse(Message):
    """Peer → peer: cached committed replica or ``None``."""

    job_id: str
    replica: GateJobReplica | None = None
    found: bool = False


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
