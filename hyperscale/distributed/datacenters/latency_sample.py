"""``LatencySample`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class LatencySample:
    """A single latency measurement for a datacenter."""

    timestamp: float
    latency_ms: float
    probe_type: str = "health"  # "health", "oob", "ping"
