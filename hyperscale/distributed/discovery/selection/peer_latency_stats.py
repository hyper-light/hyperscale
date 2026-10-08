"""``PeerLatencyStats`` -- pickled under the namespace
``hyperscale.distributed.discovery.selection.ewma_tracker`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class PeerLatencyStats:
    """Latency statistics for a single peer."""

    peer_id: str
    """The peer this tracks."""

    ewma_ms: float = 0.0
    """Current EWMA latency in milliseconds."""

    sample_count: int = 0
    """Number of samples recorded."""

    last_sample_ms: float = 0.0
    """Most recent latency sample."""

    last_updated: float = 0.0
    """Timestamp of last update (monotonic)."""

    min_ms: float = float("inf")
    """Minimum observed latency."""

    max_ms: float = 0.0
    """Maximum observed latency."""

    failure_count: int = 0
    """Number of consecutive failures (reset on success)."""
