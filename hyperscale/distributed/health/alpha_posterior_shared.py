"""Definitions shared by the classes of
``hyperscale.distributed.health.alpha_posterior`` (see that module)."""

from __future__ import annotations


# Default Beta(α, β) prior stored in every new class posterior. The
# outcome-weighted α budget reads only the EVIDENCE a posterior holds
# (``alpha - alpha_prior``, ``beta - beta_prior``; see
# ``HierarchicalAlphaTuner.alpha_budget``), so these two values act as
# an offset there and are kept unchanged: persisted AD-34 snapshots
# carry α and β with this prior folded in, and a different default
# would misread every snapshot an older manager wrote. ``posterior_mean``
# (observability) still reports the success mean under this prior.
_DEFAULT_ALPHA_PRIOR: float = 0.5

_DEFAULT_BETA_PRIOR: float = 9.5

# Pseudo-count per side of the Jeffreys prior Beta(1/2, 1/2) -- the
# reference prior for a Bernoulli rate (Jeffreys 1946; Brown, Cai &
# DasGupta 2001, Statist. Sci. 16:101) -- that keeps the POOLED
# failure rate defined and strictly inside (0, 1) before any outcome,
# and when every outcome so far is a success or every one a failure.
_JEFFREYS_PSEUDO_COUNT: float = 0.5

# Weight, in observations, of the pooled failure rate as the prior of
# each class's failure rate: the unit-information prior (Kass &
# Wasserman 1995, JASA 90:928) -- the pool counts as exactly one
# observation, so a class's own outcomes dominate from its second.
_UNIT_INFORMATION_PRIOR_WEIGHT: float = 1.0
