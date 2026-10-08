"""Wire model ``TimeoutTrackingState`` -- pickled under the wire namespace
``hyperscale.distributed.models.jobs`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field

if TYPE_CHECKING:
    from hyperscale.distributed.health.extension_ledger import ExtensionDecisionEvent
    from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent
    from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot
    from .job_info import JobInfo


@dataclass(slots=True)
class TimeoutTrackingState:
    """
    Timeout tracking state persisted in JobInfo (AD-34).

    Survives leader transfers via state sync - new leader inherits this state
    and resumes timeout tracking with incremented fence token.

    Extension Integration (AD-26):
    - total_extensions_granted: Sum of ALL extensions granted to workers in this job
    - max_worker_extension: Largest single extension granted
    - active_workers_with_extensions: Workers currently with active extensions
    - Extensions are additive: effective_timeout = timeout_seconds + total_extensions_granted
    - Extension grant = progress signal (updates last_progress_at)
    """

    strategy_type: str  # "local_authority" | "gate_coordinated"
    gate_addr: tuple[str, int] | None

    # Timestamps (absolute, monotonic)
    started_at: float  # When job started (never changes)
    last_progress_at: float  # Last workflow progress or extension
    last_report_at: float  # Last progress report to gate (multi-DC only)

    # Timeout configuration
    timeout_seconds: float
    stuck_threshold: float = 120.0  # No progress threshold (2 minutes)

    # Extension tracking (AD-26 integration)
    total_extensions_granted: float = 0.0  # Total seconds granted to ALL workers
    max_worker_extension: float = 0.0  # Largest extension granted to any worker
    last_extension_at: float = 0.0  # When last extension was granted
    # AD-26 extension seconds granted since the last observed progress:
    # each grant -- log-decaying, base / 2^n -- adds its seconds to the
    # silence AD-34 tolerates before the job counts as stuck.
    extension_seconds_since_progress: float = 0.0
    active_workers_with_extensions: set[str] = field(default_factory=set)
    # Phase H7 — per-workflow last-known progress snapshot. Replicated
    # via the AD-48 channel for real-time visibility *and* persisted
    # here so a leader takeover (AD-34 state-sync path) inherits the
    # most-recent snapshot without depending on gossip having reached
    # the new leader yet. Keys are workflow_ids; values are the
    # H3 ``WorkflowProgressSnapshot`` instances. The dict is bounded
    # implicitly by the active workflow population — entries are
    # dropped via ``ExtensionLedger.forget_workflow`` when the
    # workflow terminates.
    last_progress_snapshots: dict[str, "WorkflowProgressSnapshot"] = field(
        default_factory=dict
    )
    # Phase H7 — most-recent decision per workflow. New leader on
    # takeover sees the decision history without depending on
    # AD-48 #|x dissemination having reached them yet. Same
    # bounded-by-active-workflows discipline as
    # ``last_progress_snapshots``; entries are dropped via the
    # ledger's ``forget_workflow`` cascade on workflow termination.
    last_extension_decisions: dict[str, "ExtensionDecisionEvent"] = field(
        default_factory=dict
    )
    # Phase H8 — workflow outcomes for terminated workflows whose
    # AD-48 #|o dissemination may not have reached every peer yet.
    # On leader takeover, the new leader replays these into its
    # ``WorkerHealthManager.ingest_remote_outcome_event`` so the
    # Bayesian alpha tuner inherits the cluster's accumulated
    # learning even when the gossip channel hasn't fully converged.
    pending_extension_outcomes: dict[str, "ExtensionOutcomeEvent"] = field(
        default_factory=dict
    )
    # Phase H8 — frozen snapshot of the per-workflow-class Beta
    # posterior so the new leader inherits the entire tuner state.
    # Each entry is the ``WorkflowClassAlphaPosterior.to_bytes``
    # serialization keyed by workflow class name. Restored via
    # ``HierarchicalAlphaTuner.restore`` on takeover.
    alpha_tuner_snapshot: dict[str, bytes] = field(default_factory=dict)

    # State flags (idempotency)
    locally_timed_out: bool = False  # Manager reported/detected timeout
    globally_timed_out: bool = False  # Gate declared global timeout
    timeout_reason: str = ""

    # Fencing (prevent stale decisions after leader transfer)
    timeout_fence_token: int = 0  # Incremented on leader transfer
