"""
Hierarchical α-budget allocator (AD-26 Phase H6).

Splits a cluster-wide false-positive-rate budget down through the
DC → manager → worker → workflow hierarchy so that the family-wise
expected FPR across all extension decisions stays at the deployment-
policy ``α_system``.

Per the design conversation: Bonferroni's worst-case independence
assumption over-corrects for tests that are *not* independent
(workflows on the same worker share network, CPU, GC). Benjamini-
Hochberg FDR is asymptotically tight under positive dependence, so
the per-level split is BH-FDR proportional to the sub-level's share
of active workflows.

The hierarchy is computed online from currently-observed counts
(``active_workflows_in_dc``, ``active_workflows_on_manager``,
``active_workflows_on_worker``). No magic numbers; the only input
is ``α_system``.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

from dataclasses import dataclass

from .hierarchical_alpha_budget import _clamp_unit_interval
from .hierarchical_alpha_budget import _clamp_alpha
from .hierarchical_alpha_budget import HierarchicalAlphaBudget
from .hierarchical_alpha_config import HierarchicalAlphaConfig

_REHOMED = (
    HierarchicalAlphaConfig,
    HierarchicalAlphaBudget,
    _clamp_unit_interval,
    _clamp_alpha,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
