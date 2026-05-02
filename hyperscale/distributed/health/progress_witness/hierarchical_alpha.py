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
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class HierarchicalAlphaConfig:
    """Configuration for the hierarchical α-budget allocator."""

    # Cluster-wide tolerated FPR. Default 0.01 (1%) per the
    # production-fleet rationale: at ~24K workflows/day, 1% = ~240
    # false denies/day, an acceptable rate when extensions are
    # bounded by AD-26's max_extensions=5 cap.
    alpha_system: float = 0.01
    # Lower bound on the per-workflow α to keep the BOCPD test
    # statistically meaningful even with O(thousands) of concurrent
    # workflows. Below this floor the test becomes uselessly
    # conservative (everything passes); we accept slightly higher
    # family-wise FPR rather than blind the witness entirely.
    alpha_workflow_floor: float = 1e-5
    # Upper bound on per-workflow α — caps how aggressive the
    # witness can be even on a single-workflow cluster. Avoids
    # spurious denies during cold-start when sample counts are tiny.
    alpha_workflow_ceiling: float = 0.05


class HierarchicalAlphaBudget:
    """Allocates per-workflow α from a cluster-wide budget.

    Invocation order (call sites pre-compute the share fractions and
    pass them down):

        budget = HierarchicalAlphaBudget(HierarchicalAlphaConfig())
        alpha_dc = budget.split_to_dc(
            dc_share=0.5,             # this DC has half the cluster's active workflows
        )
        alpha_manager = budget.split_to_manager(
            alpha_dc, manager_share=0.33,
        )
        alpha_worker = budget.split_to_worker(
            alpha_manager, worker_share=0.25,
        )
        alpha_workflow = budget.split_to_workflow(
            alpha_worker, workflow_share=0.5,
        )

    Each ``split_to_*`` method enforces the floor/ceiling clamp at
    its level. ``split_to_workflow`` is the leaf — its return value
    is the per-workflow α the BOCPD test compares against.

    The call sites compute ``share`` fractions from currently-
    observed active-workflow counts:

        dc_share          = active_workflows_in_this_dc / total_active_in_cluster
        manager_share     = active_workflows_on_this_manager / total_active_in_dc
        worker_share      = active_workflows_on_this_worker / total_active_on_manager
        workflow_share    = 1 / active_workflows_on_this_worker

    With those shares the cumulative product is exactly
    ``α_system / total_active_in_cluster`` — i.e. Bonferroni at the
    cluster scale, but with the hierarchy letting individual layers
    correctly clamp at their own bounds when one tier dominates.
    """

    def __init__(self, config: HierarchicalAlphaConfig | None = None) -> None:
        self._config: HierarchicalAlphaConfig = (
            config if config is not None else HierarchicalAlphaConfig()
        )

    @property
    def config(self) -> HierarchicalAlphaConfig:
        return self._config

    def split_to_dc(self, dc_share: float) -> float:
        """Allocate the DC's slice of ``α_system``.

        ``dc_share`` is the fraction of cluster-wide active workflows
        running in this DC. A DC with 50% of the workflows gets 50%
        of the budget — straightforward proportional split since
        every workflow at every DC is equally likely to be a true
        change-point.
        """
        share = _clamp_unit_interval(dc_share)
        return self._config.alpha_system * share

    def split_to_manager(self, alpha_dc: float, manager_share: float) -> float:
        """Sub-allocate from the DC budget to a manager."""
        share = _clamp_unit_interval(manager_share)
        return alpha_dc * share

    def split_to_worker(self, alpha_manager: float, worker_share: float) -> float:
        """Sub-allocate from the manager budget to a worker."""
        share = _clamp_unit_interval(worker_share)
        return alpha_manager * share

    def split_to_workflow(
        self, alpha_worker: float, workflow_share: float
    ) -> float:
        """Leaf split. Clamps to the floor/ceiling configured at the
        budget level so per-workflow tests stay statistically
        meaningful regardless of fleet scale."""
        share = _clamp_unit_interval(workflow_share)
        raw = alpha_worker * share
        return _clamp_alpha(
            raw,
            floor=self._config.alpha_workflow_floor,
            ceiling=self._config.alpha_workflow_ceiling,
        )

    def workflow_alpha_from_counts(
        self,
        active_in_cluster: int,
        active_in_dc: int,
        active_on_manager: int,
        active_on_worker: int,
    ) -> float:
        """Convenience: compute α_workflow directly from the four
        active-workflow counts.

        Useful when call sites have the counts handy and don't need
        the per-level intermediate values for observability. Equivalent
        to chaining the four ``split_to_*`` methods. Returns the
        clamped per-workflow α.
        """
        if active_in_cluster <= 0 or active_on_worker <= 0:
            # Degenerate scale — fall back to the ceiling (most
            # permissive). Means: with zero or one concurrent
            # workflow we permit a relatively high per-test α.
            return self._config.alpha_workflow_ceiling

        dc_share = (
            active_in_dc / active_in_cluster
            if active_in_cluster > 0
            else 0.0
        )
        manager_share = (
            active_on_manager / active_in_dc
            if active_in_dc > 0
            else 0.0
        )
        worker_share = (
            active_on_worker / active_on_manager
            if active_on_manager > 0
            else 0.0
        )
        workflow_share = 1.0 / active_on_worker

        alpha_dc = self.split_to_dc(dc_share)
        alpha_manager = self.split_to_manager(alpha_dc, manager_share)
        alpha_worker = self.split_to_worker(alpha_manager, worker_share)
        return self.split_to_workflow(alpha_worker, workflow_share)


def _clamp_unit_interval(x: float) -> float:
    """Clamp ``x`` to ``[0.0, 1.0]``."""
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _clamp_alpha(x: float, floor: float, ceiling: float) -> float:
    """Clamp ``x`` to ``[floor, ceiling]``."""
    if x < floor:
        return floor
    if x > ceiling:
        return ceiling
    return x
