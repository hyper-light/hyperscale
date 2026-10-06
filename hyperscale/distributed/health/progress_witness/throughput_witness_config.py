"""``ThroughputWitnessConfig`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.throughput_witness`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass, field

from .bocpd import BOCPDConfig
from .hierarchical_alpha import HierarchicalAlphaConfig


@dataclass(slots=True, frozen=True)
class ThroughputWitnessConfig:
    """Configuration for ``ThroughputWitness``."""

    # Forwarded to the per-stream BOCPD detector.
    bocpd: BOCPDConfig = field(default_factory=BOCPDConfig)
    # Forwarded to the hierarchical α-budget allocator.
    alpha: HierarchicalAlphaConfig = field(default_factory=HierarchicalAlphaConfig)
    # Below this many samples, the witness returns COLD_START. Picked
    # so the BOCPD detector has at least a handful of samples to
    # establish a prior before its decisions are honored.
    cold_start_min_observations: int = 5
    # Maximum samples retained per (worker, workflow) for the K-S
    # adaptive-window test. Bounded so memory stays O(streams ×
    # max_history_per_stream).
    max_history_per_stream: int = 1024
    # When the K-S test rejects stationarity at this level, the
    # witness narrows the BOCPD's effective baseline to the most
    # recent half of the history. ``alpha_system`` from the budget
    # config is used by default — exposing it here lets a deployment
    # tune the K-S sensitivity independently of the FPR budget.
    ks_alpha_override: float | None = None
