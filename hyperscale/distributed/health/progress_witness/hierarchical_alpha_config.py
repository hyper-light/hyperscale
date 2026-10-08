"""``HierarchicalAlphaConfig`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.hierarchical_alpha`` (see that module)."""

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
