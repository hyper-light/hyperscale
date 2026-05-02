"""
Extension decision ledger (AD-26 Phase H7a).

Manager-side authoritative record of every extension decision made
in this cluster. Each decision becomes an ``ExtensionDecisionEvent``
that the H7b AD-48 dissemination broadcasts to peer managers, so the
whole cluster is real-time-aware of which workers/workflows are
requesting extensions and the witness evidence behind every grant
or denial.

The ledger is bounded and self-cleaning:

* ``max_decisions_per_workflow`` caps history depth per workflow at
  ``max_extensions × 2`` (default 10) — covers the full AD-26 grant
  schedule plus interleaved denies, no further.
* Workflow entries are evicted on workflow termination via
  ``forget_workflow``.
* Worker entries are evicted on worker reaping via
  ``forget_worker``.
* Job entries are evicted on job termination via ``forget_job``.

Persistence: ``ExtensionDecisionEvent`` is the wire-level type
disseminated through AD-48. The same record is also written into
``TimeoutTrackingState.last_progress_snapshots`` (per AD-34 leader
transfer) so a new leader on takeover sees the most-recent
per-workflow snapshot without depending on gossip having reached
them yet.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Iterator

from hyperscale.distributed.health.extension_decision import (
    ExtensionDecision,
    ExtensionDenialCode,
    ExtensionWitnessEvidence,
)
from hyperscale.distributed.health.workflow_progress_snapshot import (
    WorkflowProgressSnapshot,
)


# ============================================================================
# Wire-level event
# ============================================================================


@dataclass(slots=True, frozen=True)
class ExtensionDecisionEvent:
    """One extension decision as observed by a manager.

    Designed for AD-48 dissemination: every field is a primitive
    (or a frozen dataclass thereof) so the event can be safely
    pickled, hashed, and replayed on a peer manager.

    Attributes:
        job_id: Parent job identifier.
        workflow_id: Specific workflow whose extension was decided.
        worker_id: Worker that owns the workflow.
        decision: ``"granted"`` | ``"denied"``.
        denial_reason_code: Structured H5 denial code; ``"none"``
            on grant.
        denial_message: Human-readable denial detail; ``None`` on
            grant.
        extension_count_pre_decision: H1 ``extension_count`` before
            this decision was applied.
        extension_seconds: Grant amount; ``0.0`` on deny.
        cumulative_extended: Total seconds granted to this workflow
            across all extensions to date (post-decision).
        progress_snapshot: H3 snapshot reported by the worker.
        witness_evidence: H5 forensic record of every witness check.
        fence_token: AD-10/AD-34 leader-aware fence token for the
            workflow; lets peer managers reject stale events from
            superseded leaders.
        timestamp: Leader's monotonic clock at decision time.
        leader_term: Manager-cluster leadership term at decision
            time. Older terms' events are rejected on receipt.
    """

    job_id: str
    workflow_id: str
    worker_id: str
    decision: str  # "granted" | "denied"
    denial_reason_code: str  # ExtensionDenialCode value
    denial_message: str | None
    extension_count_pre_decision: int
    extension_seconds: float
    cumulative_extended: float
    progress_snapshot: WorkflowProgressSnapshot
    witness_evidence: ExtensionWitnessEvidence
    fence_token: int
    timestamp: float
    leader_term: int

    @classmethod
    def from_decision(
        cls,
        *,
        job_id: str,
        workflow_id: str,
        worker_id: str,
        decision: ExtensionDecision,
        cumulative_extended: float,
        progress_snapshot: WorkflowProgressSnapshot,
        fence_token: int,
        timestamp: float,
        leader_term: int,
    ) -> "ExtensionDecisionEvent":
        """Build an ``ExtensionDecisionEvent`` from an
        ``ExtensionDecision`` plus the surrounding context the decision
        itself doesn't carry (job_id, worker_id, fence/term)."""
        return cls(
            job_id=job_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
            decision="granted" if decision.granted else "denied",
            denial_reason_code=decision.denial_reason_code.value,
            denial_message=decision.denial_message,
            extension_count_pre_decision=(
                decision.evidence.extension_count_pre_decision
            ),
            extension_seconds=decision.extension_seconds,
            cumulative_extended=cumulative_extended,
            progress_snapshot=progress_snapshot,
            witness_evidence=decision.evidence,
            fence_token=fence_token,
            timestamp=timestamp,
            leader_term=leader_term,
        )


# ============================================================================
# Per-workflow rolling history
# ============================================================================


@dataclass(slots=True)
class ExtensionWorkflowEntry:
    """Aggregated ledger state for one workflow.

    Holds a bounded rolling history of ``ExtensionDecisionEvent``
    plus derived summary fields the manager UI / observability
    surfaces consult directly.
    """

    workflow_id: str
    job_id: str
    worker_id: str
    decisions: Deque[ExtensionDecisionEvent] = field(default_factory=deque)
    cumulative_extended: float = 0.0
    last_decision: ExtensionDecisionEvent | None = None
    last_progress_snapshot: WorkflowProgressSnapshot | None = None
    last_leader_term: int = 0

    def append(self, event: ExtensionDecisionEvent, max_decisions: int) -> None:
        """Append an event, evicting the oldest if depth exceeded."""
        self.decisions.append(event)
        while len(self.decisions) > max_decisions:
            self.decisions.popleft()
        if event.decision == "granted":
            self.cumulative_extended = event.cumulative_extended
        self.last_decision = event
        self.last_progress_snapshot = event.progress_snapshot
        self.last_leader_term = event.leader_term

    @property
    def extension_count(self) -> int:
        return sum(1 for d in self.decisions if d.decision == "granted")

    @property
    def is_exhausted(self) -> bool:
        return any(
            d.denial_reason_code == ExtensionDenialCode.MAX_EXHAUSTED.value
            for d in self.decisions
        )


# ============================================================================
# Ledger
# ============================================================================


@dataclass(slots=True)
class ExtensionLedgerConfig:
    """Configuration for ``ExtensionLedger``."""

    # Cap rolling decision history per workflow at
    # ``max_extensions × 2`` so we keep room for AD-26's full
    # 5-grant schedule plus interleaved denials but never more.
    max_decisions_per_workflow: int = 10


class ExtensionLedger:
    """Authoritative manager-side record of all extension decisions.

    Three indices for O(1) lookups:

    * ``by_workflow_id: dict[str, ExtensionWorkflowEntry]`` — primary
      keying. Workflow-scoped queries (decision history, last
      snapshot, current cumulative).
    * ``by_worker_id: dict[str, set[str]]`` — worker → set of its
      workflow_ids. Drives ``forget_worker`` cascade cleanup when a
      worker is reaped.
    * ``by_job_id: dict[str, set[str]]`` — job → set of its
      workflow_ids. Drives ``forget_job`` on job termination.

    Thread-safety: NOT thread-safe. The manager serializes through
    its existing extension lock.
    """

    def __init__(self, config: ExtensionLedgerConfig | None = None) -> None:
        self._config: ExtensionLedgerConfig = (
            config if config is not None else ExtensionLedgerConfig()
        )
        self._by_workflow_id: dict[str, ExtensionWorkflowEntry] = {}
        self._by_worker_id: dict[str, set[str]] = {}
        self._by_job_id: dict[str, set[str]] = {}

    @property
    def config(self) -> ExtensionLedgerConfig:
        return self._config

    @property
    def workflow_count(self) -> int:
        return len(self._by_workflow_id)

    # --------------------------------------------------------------
    # Mutation
    # --------------------------------------------------------------

    def record(self, event: ExtensionDecisionEvent) -> ExtensionWorkflowEntry:
        """Record a decision in the ledger.

        Idempotent on (workflow_id, fence_token, timestamp) — peer
        managers receiving the same event via AD-48 dissemination
        won't double-count it as long as the event triple matches an
        existing entry. Stale events from older leader terms are
        silently ignored.
        """
        entry = self._by_workflow_id.get(event.workflow_id)
        if entry is None:
            entry = ExtensionWorkflowEntry(
                workflow_id=event.workflow_id,
                job_id=event.job_id,
                worker_id=event.worker_id,
            )
            self._by_workflow_id[event.workflow_id] = entry
            self._by_worker_id.setdefault(event.worker_id, set()).add(
                event.workflow_id
            )
            self._by_job_id.setdefault(event.job_id, set()).add(
                event.workflow_id
            )
        elif event.leader_term < entry.last_leader_term:
            # Stale event from a superseded leader — reject.
            return entry
        elif self._is_duplicate(entry, event):
            return entry

        entry.append(event, self._config.max_decisions_per_workflow)
        return entry

    @staticmethod
    def _is_duplicate(
        entry: ExtensionWorkflowEntry, event: ExtensionDecisionEvent
    ) -> bool:
        if entry.last_decision is None:
            return False
        prev = entry.last_decision
        return (
            prev.fence_token == event.fence_token
            and prev.timestamp == event.timestamp
            and prev.decision == event.decision
        )

    def forget_workflow(self, workflow_id: str) -> None:
        """Drop ledger state for a terminated workflow."""
        entry = self._by_workflow_id.pop(workflow_id, None)
        if entry is None:
            return
        worker_set = self._by_worker_id.get(entry.worker_id)
        if worker_set is not None:
            worker_set.discard(workflow_id)
            if not worker_set:
                self._by_worker_id.pop(entry.worker_id, None)
        job_set = self._by_job_id.get(entry.job_id)
        if job_set is not None:
            job_set.discard(workflow_id)
            if not job_set:
                self._by_job_id.pop(entry.job_id, None)

    def forget_worker(self, worker_id: str) -> int:
        """Cascade-drop every workflow owned by ``worker_id``.

        Returns the count of workflows evicted.
        """
        workflow_ids = list(self._by_worker_id.get(worker_id, set()))
        for workflow_id in workflow_ids:
            self.forget_workflow(workflow_id)
        return len(workflow_ids)

    def forget_job(self, job_id: str) -> int:
        """Cascade-drop every workflow owned by ``job_id``.

        Returns the count of workflows evicted.
        """
        workflow_ids = list(self._by_job_id.get(job_id, set()))
        for workflow_id in workflow_ids:
            self.forget_workflow(workflow_id)
        return len(workflow_ids)

    # --------------------------------------------------------------
    # Read-only queries
    # --------------------------------------------------------------

    def get_workflow_entry(
        self, workflow_id: str
    ) -> ExtensionWorkflowEntry | None:
        return self._by_workflow_id.get(workflow_id)

    def workflows_for_worker(self, worker_id: str) -> list[str]:
        return sorted(self._by_worker_id.get(worker_id, set()))

    def workflows_for_job(self, job_id: str) -> list[str]:
        return sorted(self._by_job_id.get(job_id, set()))

    def iter_active_workflows(self) -> Iterator[ExtensionWorkflowEntry]:
        for entry in self._by_workflow_id.values():
            yield entry

    def workflows_with_pending_extensions(self) -> list[str]:
        """Return workflow_ids whose most-recent decision was a grant
        and whose extension count hasn't yet hit the AD-26 cap.

        Used by manager observability surfaces and the H8 outcome-
        feedback loop for "extensions in flight" filtering.
        """
        result: list[str] = []
        for entry in self._by_workflow_id.values():
            if entry.last_decision is None:
                continue
            if entry.last_decision.decision != "granted":
                continue
            if entry.is_exhausted:
                continue
            result.append(entry.workflow_id)
        return result

    def latest_progress_snapshot(
        self, workflow_id: str
    ) -> WorkflowProgressSnapshot | None:
        entry = self._by_workflow_id.get(workflow_id)
        if entry is None:
            return None
        return entry.last_progress_snapshot
