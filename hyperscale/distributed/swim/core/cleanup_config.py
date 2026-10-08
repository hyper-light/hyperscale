"""``CleanupConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.core.resource_limits`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class CleanupConfig:
    """
    Configuration for periodic cleanup of SWIM state.
    
    Used to configure garbage collection of dead nodes,
    expired suspicions, etc.
    """
    
    # Node state cleanup
    max_node_states: int = 10000
    """Maximum tracked nodes before eviction."""
    
    dead_node_retention_seconds: float = 3600.0
    """How long to remember dead nodes (for proper refutation)."""
    
    # Suspicion cleanup
    max_suspicions: int = 1000
    """Maximum concurrent suspicions."""
    
    orphaned_suspicion_timeout: float = 300.0
    """Timeout for suspicions with no timer (orphaned)."""
    
    # Gossip buffer cleanup
    max_gossip_updates: int = 1000
    """Maximum pending gossip updates."""
    
    stale_gossip_age_seconds: float = 60.0
    """Remove gossip updates older than this."""
    
    # Indirect probe cleanup  
    max_pending_probes: int = 100
    """Maximum concurrent indirect probes."""
    
    probe_retention_seconds: float = 30.0
    """How long to keep completed probe records."""
    
    # Cleanup frequency
    cleanup_interval_seconds: float = 30.0
    """How often to run cleanup."""
