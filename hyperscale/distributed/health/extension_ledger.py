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

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
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
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent, ExtensionOutcomeKind
from hyperscale.distributed.health.progress_witness import WitnessVerdictKind
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot

from .extension_decision_event import _EVENT_FIELD_COUNT
from .extension_decision_event import ExtensionDecisionEvent
from .extension_ledger_config import ExtensionLedgerConfig
from .extension_workflow_entry import ExtensionWorkflowEntry


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
        self._discard_from_index(self._by_worker_id, entry.worker_id, workflow_id)
        self._discard_from_index(self._by_job_id, entry.job_id, workflow_id)

    @staticmethod
    def _discard_from_index(
        index: dict[str, set[str]], owner_id: str, workflow_id: str
    ) -> None:
        """Remove ``workflow_id`` from ``owner_id``'s set in a worker/job
        index, dropping the set once empty so the index stays bounded."""
        workflow_set = index.get(owner_id)
        if workflow_set is None:
            return
        workflow_set.discard(workflow_id)
        if not workflow_set:
            index.pop(owner_id, None)

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
        return [
            entry.workflow_id
            for entry in self._by_workflow_id.values()
            if self._has_pending_extension(entry)
        ]

    @staticmethod
    def _has_pending_extension(entry: ExtensionWorkflowEntry) -> bool:
        """Whether the entry's latest decision was a grant and its AD-26
        extension cap is not yet hit."""
        return (
            entry.last_decision is not None
            and entry.last_decision.decision == "granted"
            and not entry.is_exhausted
        )

    def latest_progress_snapshot(
        self, workflow_id: str
    ) -> WorkflowProgressSnapshot | None:
        entry = self._by_workflow_id.get(workflow_id)
        if entry is None:
            return None
        return entry.last_progress_snapshot

_REHOMED = (
    ExtensionDecisionEvent,
    ExtensionWorkflowEntry,
    ExtensionLedgerConfig,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
