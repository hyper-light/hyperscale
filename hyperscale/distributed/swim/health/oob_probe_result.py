"""``OOBProbeResult`` -- pickled under the namespace
``hyperscale.distributed.swim.health.out_of_band_health_channel`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class OOBProbeResult:
    """Result of an out-of-band probe."""

    target: tuple[str, int]
    success: bool
    is_overloaded: bool  # True if received NACK
    latency_ms: float
    error: str | None = None
