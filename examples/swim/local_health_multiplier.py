"""
Lifeguard Local Health Multiplier (LHM) implementation.
"""

from dataclasses import dataclass


@dataclass
class LocalHealthMultiplier:
    """
    Lifeguard Local Health Multiplier (LHM).
    
    Tracks the node's own health state. A score of 0 indicates healthy,
    higher scores indicate potential issues with this node's ability
    to process messages in a timely manner.
    
    The score saturates at max_score to prevent unbounded growth.
    
    Events that increment LHM:
    - Missed nack (failed to respond in time)
    - Failed refutation (suspicion about self received)
    - Probe timeout when we initiated the probe
    
    Events that decrement LHM:
    - Successful probe round completion
    - Successful nack response received
    """
    score: int = 0
    max_score: int = 8  # Saturation limit 'S' from paper
    
    # Scoring weights for different events
    PROBE_TIMEOUT_PENALTY: int = 1
    REFUTATION_PENALTY: int = 2
    MISSED_NACK_PENALTY: int = 1
    SUCCESSFUL_PROBE_REWARD: int = 1
    SUCCESSFUL_NACK_REWARD: int = 1
    
    def increment(self, amount: int = 1) -> int:
        """
        Increment LHM score (node health is degrading).
        Returns the new score.
        """
        self.score = min(self.max_score, self.score + amount)
        return self.score
    
    def decrement(self, amount: int = 1) -> int:
        """
        Decrement LHM score (node health is improving).
        Returns the new score.
        """
        self.score = max(0, self.score - amount)
        return self.score
    
    def on_probe_timeout(self) -> int:
        """Called when a probe we sent times out."""
        return self.increment(self.PROBE_TIMEOUT_PENALTY)
    
    def on_refutation_needed(self) -> int:
        """Called when we receive a suspicion about ourselves."""
        return self.increment(self.REFUTATION_PENALTY)
    
    def on_missed_nack(self) -> int:
        """Called when we failed to respond in time."""
        return self.increment(self.MISSED_NACK_PENALTY)
    
    def on_successful_probe(self) -> int:
        """Called when a probe round completes successfully."""
        return self.decrement(self.SUCCESSFUL_PROBE_REWARD)
    
    def on_successful_nack(self) -> int:
        """Called when we successfully respond to a message."""
        return self.decrement(self.SUCCESSFUL_NACK_REWARD)
    
    def get_multiplier(self) -> float:
        """
        Get the current LHM multiplier for timeout calculations.
        
        Per Lifeguard paper, the multiplier increases probe timeout
        and suspicion timeout based on local health score.
        
        Returns a value from 1.0 (healthy) to 1 + max_score (unhealthy).
        """
        return 1.0 + (self.score / self.max_score) * self.max_score
    
    def reset(self) -> None:
        """Reset LHM to healthy state."""
        self.score = 0

