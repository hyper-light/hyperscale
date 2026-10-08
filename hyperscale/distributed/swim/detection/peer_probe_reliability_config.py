"""``PeerProbeReliabilityConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.peer_probe_reliability_tracker`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class PeerProbeReliabilityConfig:
    """Configuration for ``PeerProbeReliabilityTracker``.

    ``window_size``
        Maximum number of recent probe outcomes retained per peer. The
        empirical reliability is the success-rate over this window. A
        small window reacts faster to changes; a large window is more
        stable. The window also bounds memory usage to
        ``max_tracked_peers × window_size`` ``(timestamp, success)``
        tuples.

    ``sample_ttl_s``
        Maximum age (seconds) of a sample before it is ignored on
        read. Stale outcomes from an outage hours ago must not bias
        a current suspicion.

    ``max_tracked_peers``
        Hard cap on the number of peers tracked simultaneously. New
        peers beyond this cap are silently dropped (the call becomes a
        no-op). The cap exists to prevent memory exhaustion from
        adversarial or buggy peer churn.
    """

    window_size: int = 8
    sample_ttl_s: float = 60.0
    max_tracked_peers: int = 10000
