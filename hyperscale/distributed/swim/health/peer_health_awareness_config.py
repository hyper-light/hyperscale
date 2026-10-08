"""``PeerHealthAwarenessConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.health.peer_health_awareness`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class PeerHealthAwarenessConfig:
    """Configuration for peer health awareness."""

    # Timeout multipliers based on peer load
    # Applied on top of base probe timeout
    timeout_multiplier_busy: float = 1.25  # 25% longer for busy peers
    timeout_multiplier_stressed: float = 1.75  # 75% longer for stressed peers
    timeout_multiplier_overloaded: float = 2.5  # 150% longer for overloaded peers

    # Staleness threshold for peer health info
    stale_threshold_seconds: float = 30.0

    # Maximum peers to track (prevent memory growth)
    max_tracked_peers: int = 1000

    # Enable behavior adaptations
    enable_timeout_adaptation: bool = True
    enable_proxy_avoidance: bool = True
    enable_gossip_reduction: bool = True
