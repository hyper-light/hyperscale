"""Per-workflow-class Beta-conjugate posterior on extension success.

AD-26 Phase H8 outcome feedback loop: every time a workflow
terminates, its outcome (completed vs. timed-out / failed /
evicted) is fed into a Bayesian tuner keyed on the workflow's
Python class name. The posterior is a standard Beta(α, β) over
the success probability ``p`` of the underlying Bernoulli
"extension was warranted" trial. The posterior mean (α/(α+β))
becomes the workflow-class-level α budget that H6's
``ThroughputWitness`` uses on the next decision.

Why Beta:

* Conjugate to Bernoulli: closed-form sufficient stats means the
  update is O(1) and lossless (no sample-size truncation).
* Bounded: posterior mean stays in [0, 1] without clamping, so
  it composes naturally with the H6 hierarchical α budget which
  is also a probability.
* Persists cleanly: the posterior collapses to two floats — easy
  to ship in ``TimeoutTrackingState`` for AD-34 leader-transfer
  survivability.

Why per-class (not per-workflow-id):

* Each workflow_id is one trajectory; we'd never have enough data
  to learn anything per-id. Per-class learning aggregates across
  every load-test instance of the same Python class, so the tuner
  becomes useful within minutes on a moderately-busy cluster.
* AD-26 explicitly calls for the tuner to be "robust, performant,
  resource efficient" — the keyspace is bounded by the user's
  workflow-class count, typically O(10) to O(100) on a real
  cluster.

Initialization:

* ``alpha_prior`` and ``beta_prior`` start at the H6 floor and
  ceiling respectively. This expresses a weakly-informative prior
  centered at the H6 alpha_workflow_floor (the safest default),
  with low confidence so the data quickly dominates.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Iterator

from hyperscale.distributed.health.extension_outcome import (
    ExtensionOutcomeEvent,
    ExtensionOutcomeKind,
)


# Default Beta(α, β) prior centered at 0.05 (= 0.5 / (0.5 + 9.5))
# with very weak confidence so the first few real outcomes pull
# the posterior strongly. Chosen to match H6's default
# ``alpha_workflow_floor`` of 0.05.
_DEFAULT_ALPHA_PRIOR: float = 0.5
_DEFAULT_BETA_PRIOR: float = 9.5

# 7 ``:``-delimited fields. Used by ``to_bytes`` / ``from_bytes``
# for in-state persistence (AD-34 leader-transfer) — NOT for AD-48
# wire dissemination (the events themselves are disseminated; the
# posterior is recomputed by every follower applying those events).
_POSTERIOR_FIELD_COUNT: int = 7


@dataclass(slots=True)
class WorkflowClassAlphaPosterior:
    """Beta(α, β) posterior over success probability for one
    workflow class.

    Sufficient statistics are tracked as Welford-style running
    sums, so the order of evidence application is irrelevant and
    the same workflow-class-level posterior can be reconstructed
    on any follower as long as it's seen the same outcome events
    (which AD-48 dissemination guarantees up to broadcast hops).

    Attributes:
        workflow_class: The Python class name this posterior is
            keyed on. Free-form string; intern on read.
        alpha: Beta α parameter. ``priors_correct + 1`` after
            seeing one positive observation in the all-zeros
            initial state.
        beta: Beta β parameter. Symmetric counter for negative
            observations.
        successes: Count of positive observations applied (for
            observability — same as ``alpha - alpha_prior`` in
            steady state).
        failures: Count of negative observations applied.
        total_seen: Successes + failures.
        last_outcome_at: Monotonic timestamp of the most-recent
            outcome event applied. Used for staleness pruning.
    """

    workflow_class: str
    alpha: float = _DEFAULT_ALPHA_PRIOR
    beta: float = _DEFAULT_BETA_PRIOR
    successes: int = 0
    failures: int = 0
    total_seen: int = 0
    last_outcome_at: float = 0.0

    def apply(self, event: ExtensionOutcomeEvent) -> None:
        """Apply one outcome event to the posterior.

        UNKNOWN outcomes (still-in-flight workflows the leader hasn't
        classified) are ignored. Otherwise the Bernoulli observation
        weight is the workflow's ``final_progress_fraction`` —
        clamped to [0, 1] for safety. A workflow that timed out at
        99% progress is much weaker negative evidence than one that
        timed out at 5%, and weighting by progress preserves that.
        """
        if not event.outcome_kind.contributes_to_posterior:
            return

        weight = event.final_progress_fraction
        if weight < 0.0:
            weight = 0.0
        elif weight > 1.0:
            weight = 1.0

        if event.outcome_kind.is_success:
            # A successful workflow contributes (1 - weight) to
            # negative and weight to positive — but for COMPLETED
            # we treat the full unit as positive since the
            # workflow definitionally finished. Final progress is
            # already 1.0 in that case; the weight is informational
            # for failed outcomes.
            self.alpha += 1.0
            self.successes += 1
        else:
            # Negative observations weighted by inverse-progress so
            # workflows that died early (low weight) hit the
            # posterior harder than workflows that nearly made it.
            inverse_weight = 1.0 - weight
            self.beta += 1.0 + inverse_weight  # ≥ 1.0
            self.failures += 1

        self.total_seen += 1
        if event.completed_at > self.last_outcome_at:
            self.last_outcome_at = event.completed_at

    @property
    def posterior_mean(self) -> float:
        """E[p] = α / (α + β). Range: (0, 1)."""
        denom = self.alpha + self.beta
        if denom <= 0.0:
            return 0.0
        return self.alpha / denom

    @property
    def posterior_variance(self) -> float:
        """Var[p] = αβ / ((α+β)² (α+β+1)). Smaller variance = more
        confident posterior. Used as the bounds-tightening signal
        when composing with H6's hierarchical α budget."""
        sum_ab = self.alpha + self.beta
        if sum_ab <= 0.0:
            return 0.0
        return (self.alpha * self.beta) / (sum_ab * sum_ab * (sum_ab + 1.0))

    def alpha_budget(self, floor: float, ceiling: float) -> float:
        """Compose posterior mean with the H6 α budget bounds.

        Returns ``clamp(posterior_mean, floor, ceiling)``. Floor
        and ceiling come from ``HierarchicalAlphaConfig`` in the
        progress-witness module, so this method bridges H8's
        Bayesian learning with H6's policy bounds without H8
        needing to know about that module's internals.
        """
        mean = self.posterior_mean
        if mean < floor:
            return floor
        if mean > ceiling:
            return ceiling
        return mean

    def to_bytes(self) -> bytes:
        """Serialize for AD-34 in-state persistence.

        Format: 7 ``:``-delimited fields. ``workflow_class`` is the
        trailing free-form slot.
        """
        parts = [
            f"{self.alpha:.6f}".encode(),
            f"{self.beta:.6f}".encode(),
            str(self.successes).encode(),
            str(self.failures).encode(),
            str(self.total_seen).encode(),
            f"{self.last_outcome_at:.6f}".encode(),
            self.workflow_class.encode(),
        ]
        return b":".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkflowClassAlphaPosterior | None":
        """Deserialize from in-state persistence bytes."""
        try:
            decoded = data.decode()
            parts = decoded.split(":", maxsplit=_POSTERIOR_FIELD_COUNT - 1)
            if len(parts) < _POSTERIOR_FIELD_COUNT:
                return None
            return cls(
                workflow_class=sys.intern(parts[6]),
                alpha=float(parts[0]),
                beta=float(parts[1]),
                successes=int(parts[2]),
                failures=int(parts[3]),
                total_seen=int(parts[4]),
                last_outcome_at=float(parts[5]),
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None


@dataclass(slots=True)
class HierarchicalAlphaTunerConfig:
    """Configuration for ``HierarchicalAlphaTuner``.

    Attributes:
        max_classes: Soft cap on the number of workflow-class
            posteriors retained. Beyond this, least-recently-
            updated posteriors are evicted. Set generously: each
            posterior is ~80 bytes, so even 10k classes is < 1 MiB.
        stale_after_seconds: A class with no outcome update for
            this long is eligible for eviction when the cap is
            exceeded.
        alpha_prior: Initial α for new classes. Defaults match a
            weakly-informative prior centered on the H6 default
            ``alpha_workflow_floor``.
        beta_prior: Initial β for new classes.
    """

    max_classes: int = 10_000
    stale_after_seconds: float = 24.0 * 60.0 * 60.0  # 1 day
    alpha_prior: float = _DEFAULT_ALPHA_PRIOR
    beta_prior: float = _DEFAULT_BETA_PRIOR


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
