"""``ExtensionWorkflowEntry`` -- pickled under the namespace
``hyperscale.distributed.health.extension_ledger`` (see that module)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque
from hyperscale.distributed.health.extension_decision import ExtensionDenialCode
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot

from .extension_decision_event import ExtensionDecisionEvent


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
