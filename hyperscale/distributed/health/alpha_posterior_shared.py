"""Definitions shared by the classes of
``hyperscale.distributed.health.alpha_posterior`` (see that module)."""

from __future__ import annotations


# Default Beta(α, β) prior centered at 0.05 (= 0.5 / (0.5 + 9.5))
# with very weak confidence so the first few real outcomes pull
# the posterior strongly. Chosen to match H6's default
# ``alpha_workflow_floor`` of 0.05.
_DEFAULT_ALPHA_PRIOR: float = 0.5

_DEFAULT_BETA_PRIOR: float = 9.5
