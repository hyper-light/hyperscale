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
    # Shortest time between two samples a stream takes; 0.0 takes every
    # sample. The manager derives it from the K-S sample sizes its α
    # floor needs (``witness_feed_derivation``).
    minimum_sample_interval_seconds: float = 0.0
