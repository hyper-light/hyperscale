"""``HierarchicalAlphaTuner`` -- pickled under the namespace
``hyperscale.distributed.health.alpha_posterior`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent

from .hierarchical_alpha_tuner_config import HierarchicalAlphaTunerConfig
from .workflow_class_alpha_posterior import WorkflowClassAlphaPosterior


@dataclass(slots=True)
class HierarchicalAlphaTuner:
    """Per-workflow-class Beta posterior tuner.

    Owns a dict of ``WorkflowClassAlphaPosterior`` keyed on the
    workflow's Python class name. ``apply_outcome`` is the single
    write entry point used by both the leader (when workflows
    terminate locally) and followers (when outcome events arrive
    via AD-48 dissemination). Idempotency is the responsibility
    of the caller — ``ExtensionLedger`` only forwards each event
    once (deduped on event_id at the AD-48 layer).

    Thread-safety: NOT thread-safe. Manager serializes through
    its existing extension lock.
    """

    config: HierarchicalAlphaTunerConfig = field(
        default_factory=HierarchicalAlphaTunerConfig
    )
    _posteriors: dict[str, WorkflowClassAlphaPosterior] = field(default_factory=dict)

    def apply_outcome(self, event: ExtensionOutcomeEvent) -> None:
        """Apply one outcome event to the appropriate posterior,
        creating it on first observation.
        """
        if not event.outcome_kind.contributes_to_posterior:
            return

        posterior = self._posteriors.get(event.workflow_class)
        if posterior is None:
            posterior = WorkflowClassAlphaPosterior(
                workflow_class=event.workflow_class,
                alpha=self.config.alpha_prior,
                beta=self.config.beta_prior,
            )
            self._posteriors[event.workflow_class] = posterior
            self._maybe_evict()

        posterior.apply(event)

    def get(self, workflow_class: str) -> WorkflowClassAlphaPosterior | None:
        return self._posteriors.get(workflow_class)

    def alpha_budget(
        self, workflow_class: str, floor: float, ceiling: float
    ) -> float:
        """Composed α budget for a workflow class, falling back to
        the floor when the class hasn't been seen yet (safest
        default — minimum false-positive budget)."""
        posterior = self._posteriors.get(workflow_class)
        if posterior is None:
            return floor
        return posterior.alpha_budget(floor, ceiling)

    def __len__(self) -> int:
        return len(self._posteriors)

    def __iter__(self) -> Iterator[WorkflowClassAlphaPosterior]:
        return iter(self._posteriors.values())

    def snapshot(self) -> list[bytes]:
        """Return a serialized snapshot suitable for embedding in
        ``TimeoutTrackingState`` for AD-34 leader-transfer
        survivability. One entry per workflow class.
        """
        return [posterior.to_bytes() for posterior in self._posteriors.values()]

    def restore(self, snapshot: list[bytes]) -> int:
        """Restore from a snapshot list. Returns the number of
        posteriors successfully reconstructed (malformed entries
        are skipped silently — the leader will see them again
        through AD-48 outcome dissemination).
        """
        restored = 0
        for entry in snapshot:
            posterior = WorkflowClassAlphaPosterior.from_bytes(entry)
            if posterior is None:
                continue
            self._posteriors[posterior.workflow_class] = posterior
            restored += 1
        return restored

    def _maybe_evict(self) -> None:
        """Drop the least-recently-updated posterior if we exceed
        ``max_classes``. Stale-after gate is checked first — only
        truly idle classes are eligible — so an active workload
        with > max_classes hot classes won't thrash the tuner.
        """
        if len(self._posteriors) <= self.config.max_classes:
            return

        cutoff = self.config.stale_after_seconds
        if cutoff <= 0.0:
            return

        # Find the oldest posterior that's also past the staleness
        # cutoff relative to the newest. If no candidate qualifies,
        # we keep all entries — accuracy beats memory headroom.
        oldest_at: float | None = None
        oldest_class: str | None = None
        newest_at = 0.0
        for posterior in self._posteriors.values():
            if posterior.last_outcome_at > newest_at:
                newest_at = posterior.last_outcome_at
            if oldest_at is None or posterior.last_outcome_at < oldest_at:
                oldest_at = posterior.last_outcome_at
                oldest_class = posterior.workflow_class

        if oldest_class is None or oldest_at is None:
            return
        if newest_at - oldest_at < cutoff:
            return

        self._posteriors.pop(oldest_class, None)
