"""``HierarchicalAlphaTuner`` -- pickled under the namespace
``hyperscale.distributed.health.alpha_posterior`` (see that module)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from itertools import chain
from operator import attrgetter
from typing import Iterator
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent

from .alpha_posterior_shared import _JEFFREYS_PSEUDO_COUNT
from .alpha_posterior_shared import _UNIT_INFORMATION_PRIOR_WEIGHT
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
    of the caller: ``WorkerHealthManager`` admits each workflow's
    outcome once (``AppliedOutcomeWindow``), however many gossip
    copies of it arrive.

    ``alpha_budget`` is the read entry point: the H5 decision
    evaluator composes the H6 hierarchical α with the class's learned
    failure rate through it before every throughput-witness test.
    The tuner keeps the evidence pooled over every class it holds
    as two running sums so that read stays O(1).

    Thread-safety: NOT thread-safe. Manager serializes through
    its existing extension lock.
    """

    config: HierarchicalAlphaTunerConfig = field(
        default_factory=HierarchicalAlphaTunerConfig
    )
    _posteriors: dict[str, WorkflowClassAlphaPosterior] = field(default_factory=dict)
    # Σ over held classes of (α - alpha_prior): success evidence.
    _pooled_success_evidence: float = 0.0
    # Σ over held classes of (β - beta_prior): progress-weighted
    # failure evidence.
    _pooled_failure_evidence: float = 0.0

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

        alpha_before = posterior.alpha
        beta_before = posterior.beta
        posterior.apply(event)
        self._pooled_success_evidence += posterior.alpha - alpha_before
        self._pooled_failure_evidence += posterior.beta - beta_before
        # Evict only after the new outcome stamped ``last_outcome_at``:
        # a class created at the cap still reads 0.0 before ``apply``,
        # so evicting first dropped the newest class as the stalest.
        self._maybe_evict()

    def get(self, workflow_class: str) -> WorkflowClassAlphaPosterior | None:
        return self._posteriors.get(workflow_class)

    def alpha_budget(
        self,
        workflow_class: str,
        alpha_workflow: float,
        floor: float,
        ceiling: float,
    ) -> float:
        """The significance level of one throughput-witness test for a
        workflow of ``workflow_class``: the H6 hierarchical
        ``alpha_workflow`` re-weighted by the class's learned failure
        rate, clamped to the H6 ``[floor, ceiling]``.

        Weighted multiple testing (Genovese, Roeder & Wasserman 2006,
        Biometrika 93:509): testing hypothesis i at level ``α·w_i``
        with weights averaging 1 keeps the family error budget, and
        power is gained by giving more α to tests whose alternative
        is a priori more likely. The alternative here is "the workflow
        is stuck", so ``w = q_class / q_pool`` with ``q`` a posterior
        failure rate. Averaged over the outcomes (each class weighted
        by its evidence), those weights are 1 up to the two priors'
        pseudo-counts, so the budget H6 allocates is redistributed
        between classes rather than inflated; only the H6 clamp, a
        policy bound, departs from it.

        ``q_pool`` is the failure rate over every held class under a
        Jeffreys prior; ``q_class`` shrinks the class's own evidence
        toward ``q_pool`` with a unit-information prior (empirical
        Bayes), so a class seen once moves only part-way. A class
        with no outcomes yet would have ``q_class = q_pool`` -- weight
        1 -- so it keeps the H6 α exactly.
        """
        if (posterior := self._posteriors.get(workflow_class)) is None:
            return min(max(alpha_workflow, floor), ceiling)
        pooled_failure_rate = (self._pooled_failure_evidence + _JEFFREYS_PSEUDO_COUNT) / (
            self._pooled_success_evidence + self._pooled_failure_evidence + 2.0 * _JEFFREYS_PSEUDO_COUNT
        )
        class_success_evidence = posterior.alpha - self.config.alpha_prior
        class_failure_evidence = posterior.beta - self.config.beta_prior
        class_failure_rate = (
            class_failure_evidence + _UNIT_INFORMATION_PRIOR_WEIGHT * pooled_failure_rate
        ) / (class_success_evidence + class_failure_evidence + _UNIT_INFORMATION_PRIOR_WEIGHT)
        weighted_alpha = alpha_workflow * class_failure_rate / pooled_failure_rate
        return min(max(weighted_alpha, floor), ceiling)

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
        # A restored posterior replaces any held one of its class, so
        # the pooled evidence is re-summed rather than adjusted.
        self._pooled_success_evidence = math.fsum(
            map(attrgetter("alpha"), self._posteriors.values())
        ) - self.config.alpha_prior * len(self._posteriors)
        self._pooled_failure_evidence = math.fsum(
            map(attrgetter("beta"), self._posteriors.values())
        ) - self.config.beta_prior * len(self._posteriors)
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

        self._evict_oldest_if_stale(cutoff)

    def _evict_oldest_if_stale(self, cutoff: float) -> None:
        """Drop the least-recently-updated posterior when it lags the
        newest by at least ``cutoff`` seconds (H8 staleness gate)."""
        # Find the oldest posterior that's also past the staleness
        # cutoff relative to the newest. If no candidate qualifies,
        # we keep all entries — accuracy beats memory headroom.
        # ``min``/``max`` keep the first extreme in iteration order and
        # compare exactly as the former single-pass scan did.
        oldest_posterior = min(
            self._posteriors.values(),
            key=attrgetter("last_outcome_at"),
            default=None,
        )
        if oldest_posterior is None:
            return
        newest_at = max(
            chain((0.0,), map(attrgetter("last_outcome_at"), self._posteriors.values()))
        )
        if newest_at - oldest_posterior.last_outcome_at < cutoff:
            return

        del self._posteriors[oldest_posterior.workflow_class]
        self._pooled_success_evidence -= oldest_posterior.alpha - self.config.alpha_prior
        self._pooled_failure_evidence -= oldest_posterior.beta - self.config.beta_prior
