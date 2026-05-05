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

    # Per architecture.md line 7221:
    #     effective_timeout = base_timeout × (1 + LHM_score × MULTIPLIER_WEIGHT)
    # Centralised here so callers (probe-path adjustment, suspicion-
    # bracket composition) can derive the saturation cap without
    # duplicating the constant.
    MULTIPLIER_WEIGHT: float = 0.25

    # Scoring weights for different events
    # Per Lifeguard paper (Section 4.3): all events are +1 or -1
    PROBE_TIMEOUT_PENALTY: int = 1
    REFUTATION_PENALTY: int = 1  # Paper: "Refuting a suspect message about self: +1"
    MISSED_NACK_PENALTY: int = 1
    EVENT_LOOP_LAG_PENALTY: int = 1
    EVENT_LOOP_CRITICAL_PENALTY: int = 1  # Per Lifeguard paper: all penalties are +1
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

        Authoritative source: ``docs/architecture.md`` line 7221:
        ``effective_timeout = base_timeout × (1 + LHM_score × 0.25)``

        With ``max_score=8`` (S=8), this gives a multiplier range of
        [1.0, 3.0] — well below the raw Lifeguard-paper formula
        ``ProbeTimeout = BaseProbeTimeout × (LHM + 1)`` which would
        produce [1.0, 9.0].

        The architecture-doc formula is corroborated by:

        * ``architecture.md`` lines 7140–7155 (Backpressure & Degradation
          table): NORMAL 1.0×, ELEVATED 1.25×, HIGH 1.5×, SEVERE 2×,
          CRITICAL 3×. Endpoints match ``1 + score × 0.25`` exactly.
        * ``JobSuspicionConfig.max_lhm_backoff_multiplier = 3.0``
          (``job_suspicion_manager.py:45``) — the job layer already
          caps LHM at 3.0 when applying it to poll intervals.
        * AD-35 §"For Managers" line 352 worked example uses
          ``2.5 × LHM`` (in [1, 3]).

        The paper-formula range [1, 9] would push suspicion timers and
        probe timeouts off the cliff under sustained probe failures.
        Returning the doc-formula here unifies all consumers (probe
        path via ``get_lhm_adjusted_timeout``, suspicion timer via
        ``HierarchicalFailureDetector.suspect_global``, job-layer
        polling) on a single deployment-stable scale.

        Callers that want the raw 0–8 score (e.g. ``cross_dc_correlation``
        for systemic-load detection, ``leader_eligibility`` for
        candidate ranking) read ``self.score`` directly — keep the raw
        signal and the timeout multiplier conceptually separate.
        """
        return 1.0 + (self.score * self.MULTIPLIER_WEIGHT)

    def get_max_multiplier(self) -> float:
        """Return the multiplier value at full LHM saturation.

        With the doc-formula weights this equals ``1 + max_score *
        MULTIPLIER_WEIGHT`` (default 3.0). Probe-path bounded
        composition uses this as the upper cap on uncertainty padding
        — see ``HealthAwareServer.get_lhm_adjusted_timeout``.
        """
        return 1.0 + (self.max_score * self.MULTIPLIER_WEIGHT)
    
    def reset(self) -> None:
        """Reset LHM to healthy state."""
        self.score = 0

