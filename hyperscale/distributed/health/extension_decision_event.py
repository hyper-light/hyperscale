"""``ExtensionDecisionEvent`` -- pickled under the namespace
``hyperscale.distributed.health.extension_ledger`` (see that module)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from hyperscale.distributed.health.extension_decision import ExtensionDecision, ExtensionWitnessEvidence
from hyperscale.distributed.health.progress_witness import WitnessVerdictKind
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot

# Field count for ``ExtensionDecisionEvent.to_bytes``. The denial-
# message field is the trailing variable-content slot, so the
# decoder uses ``maxsplit = _EVENT_FIELD_COUNT - 1`` to keep any
# embedded ``:`` characters intact in the message.
_EVENT_FIELD_COUNT: int = 29


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
