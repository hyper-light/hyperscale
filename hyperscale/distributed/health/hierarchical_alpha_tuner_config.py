"""``HierarchicalAlphaTunerConfig`` -- pickled under the namespace
``hyperscale.distributed.health.alpha_posterior`` (see that module)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass

from .alpha_posterior_shared import _DEFAULT_ALPHA_PRIOR
from .alpha_posterior_shared import _DEFAULT_BETA_PRIOR

if TYPE_CHECKING:
    from .hierarchical_alpha_tuner import HierarchicalAlphaTuner


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
