"""``WorkflowClassAlphaPosterior`` -- pickled under the namespace
``hyperscale.distributed.health.alpha_posterior`` (see that module)."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent

from .alpha_posterior_shared import _DEFAULT_ALPHA_PRIOR
from .alpha_posterior_shared import _DEFAULT_BETA_PRIOR

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
        """E[p] = α / (α + β), the success mean under the stored prior.
        Range: (0, 1). Observability only: the outcome-weighted α
        budget (``HierarchicalAlphaTuner.alpha_budget``) reads the
        evidence net of the prior."""
        denom = self.alpha + self.beta
        if denom <= 0.0:
            return 0.0
        return self.alpha / denom

    @property
    def posterior_variance(self) -> float:
        """Var[p] = αβ / ((α+β)² (α+β+1)). Smaller variance = more
        confident posterior. Observability only."""
        sum_ab = self.alpha + self.beta
        if sum_ab <= 0.0:
            return 0.0
        return (self.alpha * self.beta) / (sum_ab * sum_ab * (sum_ab + 1.0))

    def to_bytes(self) -> bytes:
        """Serialize for AD-34 in-state persistence.

        Format: 7 ``:``-delimited fields. ``workflow_class`` is the
        trailing free-form slot. Floats are written as ``repr`` -- the
        shortest string that parses back to the same float -- so a
        new leader restores the posterior bit-for-bit and budgets α
        exactly as the old one did (``from_bytes`` parses any float
        text, so snapshots written at six decimals still load).
        """
        parts = [
            repr(self.alpha).encode(),
            repr(self.beta).encode(),
            str(self.successes).encode(),
            str(self.failures).encode(),
            str(self.total_seen).encode(),
            repr(self.last_outcome_at).encode(),
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
