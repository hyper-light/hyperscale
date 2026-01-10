from dataclasses import dataclass, field
import time


@dataclass(slots=True)
class NetworkCoordinate:
    """Network coordinate for RTT estimation."""

    vec: list[float]
    height: float
    adjustment: float
    error: float
    updated_at: float = field(default_factory=time.monotonic)
    sample_count: int = 0
