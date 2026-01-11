"""
Leader eligibility determination for LHM-aware leadership.
"""

from dataclasses import dataclass
from ..core.types import Status


@dataclass(slots=True)
class LeaderEligibility:
    """
    Determines if a node can become or remain a leader.
    
    Integrates with Lifeguard's Local Health Multiplier (LHM) to ensure
    that overloaded nodes do not become leaders. This is critical for
    multi-datacenter deployments where nodes may consume high CPU/memory.
    
    A node is eligible for leadership if:
    1. Its status is ALIVE (not SUSPECT or DEAD)
    2. Its LHM score is below the threshold
    3. It is not currently suspected by others
    """
    # Maximum LHM score for a node to be eligible as leader
    # LHM ranges from 0 (healthy) to max_score (8 by default)
    max_leader_lhm: int = 2
    
    # Minimum members required for election (quorum)
    min_members_for_election: int = 1
    
    def is_eligible(
        self, 
        lhm_score: int, 
        status: Status,
        is_suspected: bool = False,
    ) -> bool:
        """Check if a node with given state can become leader."""
        return (
            status == b'OK' and
            lhm_score <= self.max_leader_lhm and
            not is_suspected
        )
    
    def should_step_down(self, current_lhm: int) -> bool:
        """
        Check if current leader should voluntarily step down.
        Called when leader's LHM increases due to load.
        """
        return current_lhm > self.max_leader_lhm
    
    def get_leader_priority(
        self, 
        node: tuple[str, int], 
        incarnation: int, 
        lhm_score: int,
    ) -> tuple[int, int, str, int]:
        """
        Get priority tuple for leader selection.
        Lower tuple = higher priority for leadership.
        
        Priority order:
        1. Lower LHM score (healthier nodes preferred)
        2. Higher incarnation (more "proven" nodes)
        3. Lower address (deterministic tie-breaker)
        """
        return (lhm_score, -incarnation, node[0], node[1])

