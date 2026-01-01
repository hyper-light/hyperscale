"""
Suspicion state tracking for Lifeguard protocol.
"""

import asyncio
import math
import time
from dataclasses import dataclass, field


@dataclass
class SuspicionState:
    """
    Tracks the suspicion state for a single node.
    
    Per Lifeguard paper, the suspicion timeout is dynamically calculated as:
    timeout = max(min_timeout, (max_timeout - min_timeout) * log(C+1) / log(N+1))
    
    Where:
    - C is the number of independent confirmations
    - N is the total number of members in the group
    
    The timeout decreases as more confirmations are received, but never
    goes below min_timeout.
    """
    node: tuple[str, int]
    incarnation: int
    start_time: float
    confirmers: set[tuple[str, int]] = field(default_factory=set)
    min_timeout: float = 1.0
    max_timeout: float = 10.0
    n_members: int = 1
    # Lifeguard re-gossip factor K: number of times to re-gossip suspicion
    regossip_factor: int = 3
    regossip_count: int = 0
    _timer_task: asyncio.Task | None = field(default=None, repr=False)
    
    def add_confirmation(self, from_node: tuple[str, int]) -> bool:
        """
        Add a confirmation from another node.
        Returns True if this is a new confirmation.
        """
        if from_node in self.confirmers:
            return False
        self.confirmers.add(from_node)
        return True
    
    @property
    def confirmation_count(self) -> int:
        """Number of independent confirmations received."""
        return len(self.confirmers)
    
    def calculate_timeout(self) -> float:
        """
        Calculate the current suspicion timeout based on confirmations.
        
        Uses the Lifeguard formula:
        timeout = max(min, (max - min) * log(C+1) / log(N+1))
        
        More confirmations = lower timeout (faster declaration of failure)
        """
        c = self.confirmation_count
        n = max(1, self.n_members)
        
        if n <= 1:
            return self.max_timeout
        
        # Lifeguard formula from the paper
        log_factor = math.log(c + 1) / math.log(n + 1)
        timeout = self.max_timeout - (self.max_timeout - self.min_timeout) * log_factor
        
        return max(self.min_timeout, timeout)
    
    def time_remaining(self) -> float:
        """Calculate time remaining before suspicion expires."""
        elapsed = time.monotonic() - self.start_time
        timeout = self.calculate_timeout()
        return max(0, timeout - elapsed)
    
    def is_expired(self) -> bool:
        """Check if the suspicion has expired (node should be marked DEAD)."""
        return self.time_remaining() <= 0
    
    def should_regossip(self) -> bool:
        """Check if we should re-gossip this suspicion."""
        return self.regossip_count < self.regossip_factor
    
    def mark_regossiped(self) -> None:
        """Mark that we've re-gossiped this suspicion."""
        self.regossip_count += 1
    
    def cancel_timer(self) -> None:
        """Cancel the expiration timer if running."""
        if self._timer_task and not self._timer_task.done():
            self._timer_task.cancel()
            self._timer_task = None

