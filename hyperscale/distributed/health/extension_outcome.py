"""Extension outcome events (AD-26 Phase H8a).

When a workflow terminates, the leader records ``how it ended`` —
completed normally, timed out, failed, or was evicted — and pairs
the outcome with the cluster-wide record of every extension
decision that workflow accumulated. The outcome event is the
training signal for the H8 Bayesian alpha-tuner: workflows that
extended-and-completed shrink the false-positive budget for that
workflow class; workflows that extended-and-timed-out widen it so
future requests of the same class face stricter scrutiny.

Wire-level events are disseminated via AD-48 piggyback on a new
``#|o`` channel parallel to H7b's ``#|x`` decision channel. The
on-the-wire format is ``:``-delimited primitives so the event can
be safely pickled, hashed, and replayed on a peer manager.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum


class ExtensionOutcomeKind(Enum):
    """How a workflow with extension history terminated.

    The Bayesian tuner treats COMPLETED as a positive Bernoulli
    observation (the extension(s) helped) and TIMED_OUT / FAILED /
    EVICTED as negative observations (the extension(s) didn't
    rescue the workflow). UNKNOWN is reserved for outcomes we
    haven't classified yet — it does not contribute to the
    posterior.
    """

    UNKNOWN = "unknown"
    COMPLETED = "completed"
    TIMED_OUT = "timed_out"
    FAILED = "failed"
    EVICTED = "evicted"

    @property
    def is_success(self) -> bool:
        """True iff this outcome counts as evidence the extensions
        were warranted."""
        return self is ExtensionOutcomeKind.COMPLETED

    @property
    def contributes_to_posterior(self) -> bool:
        """True iff this outcome is informative for the Bayesian
        tuner (i.e. excludes UNKNOWN)."""
        return self is not ExtensionOutcomeKind.UNKNOWN


# 13 ``:``-delimited fields. The last field is workflow_class —
# allowed to contain ``:`` since user class names theoretically
# could (e.g. namespaced naming). Use ``maxsplit = 12`` on decode.
_OUTCOME_FIELD_COUNT: int = 13


@dataclass(slots=True, frozen=True)
class ExtensionOutcomeEvent:
    """One workflow-termination outcome paired with extension stats.

    Designed for AD-48 dissemination: every field is a primitive
    so the event survives pickling, hashing, and replay on a peer.

    Attributes:
        job_id: Parent job identifier.
        workflow_id: Specific workflow whose termination is being
            reported.
        workflow_class: Stable name of the workflow's Python class
            (e.g. ``LoadTestHomepage``). The Bayesian tuner keys
            its Beta posterior on this so cross-instance learning
            accumulates: every ``LoadTestHomepage`` run adds to the
            same posterior.
        worker_id: Worker that owned the workflow at termination.
        outcome_kind: Structured ``ExtensionOutcomeKind`` value.
        granted_extension_count: Number of grants this workflow
            received over its lifetime.
        denied_extension_count: Number of denials.
        total_extended_seconds: Sum of grant amounts (post-decision
            running total).
        final_progress_fraction: Worker's last-reported
            ``cores_completed / cores_total`` ratio at termination.
            Range [0.0, 1.0]. The tuner uses this to weight the
            evidence — a workflow that extended and got 99% of the
            way there is a different signal from one that timed
            out at 5%.
        completed_at: Leader's monotonic clock at outcome time.
        fence_token: AD-10/AD-34 leader-aware fence token.
        leader_term: Manager-cluster leadership term at outcome time.
    """

    job_id: str
    workflow_id: str
    workflow_class: str
    worker_id: str
    outcome_kind: ExtensionOutcomeKind
    granted_extension_count: int
    denied_extension_count: int
    total_extended_seconds: float
    final_progress_fraction: float
    completed_at: float
    fence_token: int
    leader_term: int

    def to_bytes(self) -> bytes:
        """Serialize for AD-48 piggyback dissemination.

        Format: 13 ``:``-delimited fields. The trailing field is
        ``workflow_class`` (the only variable-content slot we
        intentionally allow free-form), so the decoder uses
        ``maxsplit = 12`` to preserve any embedded ``:`` characters.
        """
        parts = [
            self.job_id.encode(),
            self.workflow_id.encode(),
            self.worker_id.encode(),
            self.outcome_kind.value.encode(),
            str(self.granted_extension_count).encode(),
            str(self.denied_extension_count).encode(),
            f"{self.total_extended_seconds:.6f}".encode(),
            f"{self.final_progress_fraction:.6f}".encode(),
            f"{self.completed_at:.6f}".encode(),
            str(self.fence_token).encode(),
            str(self.leader_term).encode(),
            # Pad with one constant field so the workflow_class
            # remains the trailing free-form slot. Reserved for a
            # future dimension without breaking wire compatibility.
            b"-",
            self.workflow_class.encode(),
        ]
        return b":".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "ExtensionOutcomeEvent | None":
        """Deserialize an outcome event from piggyback bytes.

        Returns ``None`` on any structural parse error so a single
        corrupt entry can't break a whole frame. Stale-term and
        idempotency checks are downstream in the ledger, not here.
        """
        try:
            decoded = data.decode()
            parts = decoded.split(":", maxsplit=_OUTCOME_FIELD_COUNT - 1)
            if len(parts) < _OUTCOME_FIELD_COUNT:
                return None
            return cls(
                job_id=sys.intern(parts[0]),
                workflow_id=sys.intern(parts[1]),
                worker_id=sys.intern(parts[2]),
                outcome_kind=ExtensionOutcomeKind(parts[3]),
                granted_extension_count=int(parts[4]),
                denied_extension_count=int(parts[5]),
                total_extended_seconds=float(parts[6]),
                final_progress_fraction=float(parts[7]),
                completed_at=float(parts[8]),
                fence_token=int(parts[9]),
                leader_term=int(parts[10]),
                workflow_class=sys.intern(parts[12]),
            )
        except (ValueError, KeyError, UnicodeDecodeError, IndexError):
            return None

    @property
    def event_id(self) -> str:
        """Stable per-outcome identifier for piggyback bookkeeping.

        Workflows have at most one outcome (a workflow either
        completes, times out, fails, or is evicted — exactly once),
        so the workflow_id alone is the dedup key. This is
        different from the per-decision event_id on
        ``ExtensionDecisionEvent`` which keys on the (workflow_id,
        fence_token, timestamp) triple.
        """
        return self.workflow_id
