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

import sys
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Iterator

from hyperscale.distributed.health.extension_decision import (
    ExtensionDecision,
    ExtensionDenialCode,
    ExtensionWitnessEvidence,
)
from hyperscale.distributed.health.extension_outcome import (
    ExtensionOutcomeEvent,
    ExtensionOutcomeKind,
)
from hyperscale.distributed.health.progress_witness import (
    WitnessVerdictKind,
)
from hyperscale.distributed.health.workflow_progress_snapshot import (
    WorkflowProgressSnapshot,
)


# Field count for ``ExtensionDecisionEvent.to_bytes``. The denial-
# message field is the trailing variable-content slot, so the
# decoder uses ``maxsplit = _EVENT_FIELD_COUNT - 1`` to keep any
# embedded ``:`` characters intact in the message.
_EVENT_FIELD_COUNT: int = 29


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

    def to_bytes(self) -> bytes:
        """Serialize for AD-48 piggyback dissemination.

        Format: 29 ``:``-delimited fields. The trailing field is
        ``denial_message`` (the only variable-content slot), so the
        decoder uses ``maxsplit = 28`` to preserve any embedded ``:``
        characters in the message. The snapshot's ``workflow_id`` is
        omitted — it's identical to the event's ``workflow_id`` and
        reconstructed during ``from_bytes``.
        """
        snapshot = self.progress_snapshot
        evidence = self.witness_evidence
        parts = [
            self.job_id.encode(),
            self.workflow_id.encode(),
            self.worker_id.encode(),
            self.decision.encode(),
            self.denial_reason_code.encode(),
            b"1" if self.denial_message is not None else b"0",
            str(self.extension_count_pre_decision).encode(),
            f"{self.extension_seconds:.6f}".encode(),
            f"{self.cumulative_extended:.6f}".encode(),
            str(self.fence_token).encode(),
            f"{self.timestamp:.6f}".encode(),
            str(self.leader_term).encode(),
            str(snapshot.cores_completed).encode(),
            str(snapshot.cores_total).encode(),
            str(snapshot.step_transitions).encode(),
            str(snapshot.actions_completed).encode(),
            f"{snapshot.snapshot_time:.6f}".encode(),
            b"1" if evidence.progress_meaningful else b"0",
            b"1" if evidence.progress_all_non_regressed else b"0",
            b"1" if evidence.progress_any_advanced else b"0",
            evidence.throughput_verdict_kind.name.encode(),
            f"{evidence.throughput_change_point_probability:.6f}".encode(),
            f"{evidence.throughput_alpha_workflow:.6f}".encode(),
            f"{evidence.throughput_predictive_mean_before:.6f}".encode(),
            f"{evidence.throughput_predictive_mean_after:.6f}".encode(),
            evidence.overload_state.encode(),
            f"{evidence.seconds_since_last_extension:.6f}".encode(),
            str(evidence.extension_count_pre_decision).encode(),
            (self.denial_message or "").encode(),
        ]
        return b":".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "ExtensionDecisionEvent | None":
        """Deserialize an event from AD-48 piggyback bytes.

        Returns ``None`` if the payload is malformed (wrong field
        count, unparseable enum, etc.). Idempotency and stale-term
        rejection happen downstream in ``ExtensionLedger.record``,
        not here.
        """
        try:
            decoded = data.decode()
            parts = decoded.split(":", maxsplit=_EVENT_FIELD_COUNT - 1)
            if len(parts) < _EVENT_FIELD_COUNT:
                return None

            workflow_id = sys.intern(parts[1])
            denial_message_present = parts[5] == "1"
            denial_message_text = parts[28]
            denial_message = denial_message_text if denial_message_present else None

            snapshot = WorkflowProgressSnapshot(
                workflow_id=workflow_id,
                cores_completed=int(parts[12]),
                cores_total=int(parts[13]),
                step_transitions=int(parts[14]),
                actions_completed=int(parts[15]),
                snapshot_time=float(parts[16]),
            )
            evidence = ExtensionWitnessEvidence(
                progress_meaningful=parts[17] == "1",
                progress_all_non_regressed=parts[18] == "1",
                progress_any_advanced=parts[19] == "1",
                throughput_verdict_kind=WitnessVerdictKind[parts[20]],
                throughput_change_point_probability=float(parts[21]),
                throughput_alpha_workflow=float(parts[22]),
                throughput_predictive_mean_before=float(parts[23]),
                throughput_predictive_mean_after=float(parts[24]),
                overload_state=parts[25],
                seconds_since_last_extension=float(parts[26]),
                extension_count_pre_decision=int(parts[27]),
            )
            return cls(
                job_id=sys.intern(parts[0]),
                workflow_id=workflow_id,
                worker_id=sys.intern(parts[2]),
                decision=parts[3],
                denial_reason_code=parts[4],
                denial_message=denial_message,
                extension_count_pre_decision=int(parts[6]),
                extension_seconds=float(parts[7]),
                cumulative_extended=float(parts[8]),
                progress_snapshot=snapshot,
                witness_evidence=evidence,
                fence_token=int(parts[9]),
                timestamp=float(parts[10]),
                leader_term=int(parts[11]),
            )
        except (ValueError, KeyError, UnicodeDecodeError, IndexError):
            return None

    @property
    def event_id(self) -> str:
        """Stable per-decision identifier for piggyback bookkeeping.

        AD-48 dissemination uses this as the dict key in
        ``ExtensionDecisionGossipBuffer.updates`` so the same decision
        replayed via gossip from multiple peers collapses to one
        broadcast slot. The triple is unique because the manager's
        leader-term advances monotonically and the decision timestamp
        is the leader's monotonic clock.
        """
        return f"{self.workflow_id}|{self.fence_token}|{self.timestamp:.6f}"


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
    # Phase H8: outcome paired with the decision history. ``None``
    # while the workflow is in flight; populated exactly once when
    # the leader records a termination via ``record_outcome``.
    outcome: ExtensionOutcomeEvent | None = None

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
    def denial_count(self) -> int:
        return sum(1 for d in self.decisions if d.decision == "denied")

    @property
    def is_exhausted(self) -> bool:
        return any(
            d.denial_reason_code == ExtensionDenialCode.MAX_EXHAUSTED.value
            for d in self.decisions
        )

    @property
    def is_terminated(self) -> bool:
        """True iff a Phase H8 outcome event has been applied."""
        return self.outcome is not None


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

    def record_outcome(
        self, event: ExtensionOutcomeEvent
    ) -> ExtensionWorkflowEntry | None:
        """Phase H8: record a workflow termination outcome.

        Idempotent: a second outcome with the same workflow_id is
        accepted only if it carries a higher ``leader_term`` (a
        new leader's authoritative version supersedes a stale one).
        Stale-term events are rejected.

        Returns the updated entry, or ``None`` if no decision
        history exists for this workflow_id (extension never
        requested → nothing to learn from). The caller should
        still feed the event into the alpha tuner directly in
        that case if they want to record the no-extension outcome.
        """
        entry = self._by_workflow_id.get(event.workflow_id)
        if entry is None:
            return None

        if entry.outcome is not None:
            if event.leader_term <= entry.outcome.leader_term:
                return entry

        entry.outcome = event
        if event.leader_term > entry.last_leader_term:
            entry.last_leader_term = event.leader_term
        return entry

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
