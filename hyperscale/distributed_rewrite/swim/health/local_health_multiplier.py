"""
Lifeguard Local Health Multiplier (LHM) implementation.
"""

from dataclasses import dataclass


@dataclass(slots=True)
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
    # Per Lifeguard paper (Section 4.3): all events are +1 or -1
    PROBE_TIMEOUT_PENALTY: int = 1
    REFUTATION_PENALTY: int = 1  # Paper: "Refuting a suspect message about self: +1"
    MISSED_NACK_PENALTY: int = 1
    EVENT_LOOP_LAG_PENALTY: int = 1
    EVENT_LOOP_CRITICAL_PENALTY: int = 2
    SUCCESSFUL_PROBE_REWARD: int = 1
    SUCCESSFUL_NACK_REWARD: int = 1
    EVENT_LOOP_RECOVERED_REWARD: int = 1
    
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
    
    def on_event_loop_lag(self) -> int:
        """Called when event loop lag is detected (proactive)."""
        return self.increment(self.EVENT_LOOP_LAG_PENALTY)
    
    def on_event_loop_critical(self) -> int:
        """Called when event loop is critically overloaded."""
        return self.increment(self.EVENT_LOOP_CRITICAL_PENALTY)
    
    def on_event_loop_recovered(self) -> int:
        """Called when event loop recovers from degraded state."""
        return self.decrement(self.EVENT_LOOP_RECOVERED_REWARD)
    
    def get_multiplier(self) -> float:
        """
        Get the current LHM multiplier for timeout calculations.

        Per Lifeguard paper (Section 4.3, page 5):
        "ProbeTimeout = BaseProbeTimeout × (LHM(S) + 1)"

        With max_score=8 (S=8), this gives a multiplier range of 1-9.
        The paper states: "S defaults to 8, which means the probe interval
        and timeout will back off as high as 9 seconds and 4.5 seconds"
        (from base values of 1 second and 500ms respectively).

        Returns a value from 1.0 (healthy, score=0) to 9.0 (max unhealthy, score=8).
        """
        return 1.0 + self.score
    
    def reset(self) -> None:
        """Reset LHM to healthy state."""
        self.score = 0

