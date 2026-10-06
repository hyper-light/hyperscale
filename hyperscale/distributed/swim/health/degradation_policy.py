"""``DegradationPolicy`` -- pickled under the namespace
``hyperscale.distributed.swim.health.graceful_degradation`` (see that module)."""

from dataclasses import dataclass


@dataclass(slots=True)
class DegradationPolicy:
    """
    Policy for graceful degradation behavior at each level.
    
    Higher degradation levels progressively shed more load while
    maintaining core functionality (responding to probes, gossip).
    """
    
    # Probe rate multiplier (1.0 = normal, 0.5 = half rate)
    probe_rate: float = 1.0
    
    # Gossip rate multiplier
    gossip_rate: float = 1.0
    
    # Max piggyback updates per message
    max_piggyback_updates: int = 5
    
    # Timeout multiplier (extends all timeouts)
    timeout_multiplier: float = 1.0
    
    # Should step down from leadership
    should_step_down: bool = False
    
    # Should refuse leadership candidacy
    refuse_leadership: bool = False
    
    # Skip indirect probing when overloaded
    skip_indirect_probing: bool = False
    
    # Description for logging
    description: str = ""
