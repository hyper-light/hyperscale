"""Wire model ``GateJobReplica`` -- pickled under the wire namespace
``hyperscale.distributed.models.gate_replication`` (see that module)."""

from dataclasses import dataclass, field

from .datacenter_substitution import DatacenterSubstitution
from .message import Message


@dataclass(slots=True)
class GateJobReplica(Message):
    """The takeover capsule.

    Carries everything a peer gate needs to assume leadership of a job
    when the accepting gate dies. Designed for idempotent apply: peers
    track ``(job_id, sequence)`` and ignore re-deliveries.

    Fields map directly into ``GateJobManager`` / ``GateRuntimeState``
    state on the peer:

    * ``status_seed``, ``submitted_wall_time`` → ``GlobalJobStatus``
      written into ``_jobs[job_id]``. The submission instant travels as
      wall-clock time: each gate's monotonic clock has its own epoch, so
      one gate's monotonic reading is meaningless on another -- the
      receiver converts it to its own monotonic base.
    * ``target_dcs`` → ``_job_target_dcs[job_id]``.
    * ``callback_addr`` → ``_job_callbacks[job_id]`` and
      ``_progress_callbacks[job_id]``.
    * ``fence_token`` → ``_job_fence_tokens[job_id]`` and the
      leadership-tracker fencing token via
      ``apply_leadership``.
    * ``workflow_ids`` → ``_job_workflow_ids[job_id]``.
    * ``submission_payload`` → ``_job_submissions[job_id]`` (raw
      serialized ``JobSubmission`` so peers do not need to deserialize
      the submission unless they actually take over and dispatch).
    * ``leader_id`` + ``leader_addr`` →
      ``_job_leadership_tracker.apply_leadership``.
    * ``origin_gate_addr`` is the gate that accepted the client
      submission (== ``leader_addr`` at submission time, but tracked
      separately so it survives later leader changes).
    * ``datacenter_substitutions`` → each datacenter the job lost while
      it ran, the one its unfinished workflows re-ran in and the
      workflows whose results it delivered first (AD-36 Part 13):
      whichever gate leads the job aggregates by the same result slots.
    * ``released_datacenters`` → datacenters the job no longer runs in
      that must stop running it (lost, or dispatched to without an
      answer): told to cancel it until they confirm.
    * ``raft_voters`` → the voters of the job's Raft group, decided by the
      accepting gate and the same on every gate that joins the group
      (AD-52): gates that differed in them could each count a quorum the
      others would not.
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
    submitted_wall_time: float
    raft_voters: list[str]
    workflow_ids: list[str] = field(default_factory=list)
    submission_payload: bytes = b""
    datacenter_substitutions: list[DatacenterSubstitution] = field(default_factory=list)
    released_datacenters: list[str] = field(default_factory=list)
    # AD-40: the submission's idempotency key (empty when it had none).
    # Every gate committing the replica records the key as decided for
    # this job, and no gate prepares a replica of another job under it --
    # a retry of the submission at any gate, even after its accepting gate
    # died, is answered for this job instead of admitting a second one.
    idempotency_key: str = ""
    # AD-44 late-result ``update`` policy: the latest result the job's
    # leader handed its client provisionally (a serialized
    # ``GlobalJobResult``, empty when none went out), committed here before
    # it was pushed, and the wall-clock instant its best-effort window
    # closes. A gate that leads the job after a restart or a takeover
    # resumes the window from them, so the job's final result holds every
    # datacenter result the client already received.
    provisional_result: bytes = b""
    provisional_deadline_wall_time: float = 0.0
