"""
Suspicion state tracking for Lifeguard protocol.
"""

import asyncio
import math
import time
from dataclasses import dataclass, field

# Maximum confirmers to track per suspicion
# In practice, confirmers should never exceed cluster size
MAX_CONFIRMERS = 1000


@dataclass(slots=True)
class SuspicionState:
    """
    Tracks the suspicion state for a single node.
    
    Per Lifeguard/memberlist, the suspicion timeout is dynamically
    calculated as:
    timeout = max_timeout - (max_timeout - min_timeout) * log(C + 1) / log(K + 1)

    Where:
    - C is the number of independent confirmations
    - K is the configured confirmation target. If unset, K falls back
      to the total member count for compatibility with older callers.
    
    The timeout decreases as more confirmations are received, but never
    goes below min_timeout.
    
    Memory safety:
    - Confirmers set is bounded to max_confirmers
    - Extra confirmations beyond limit are dropped but still affect timeout
    
    Uses __slots__ for memory efficiency since many instances may be created
    during failure scenarios.
    """
    node: tuple[str, int]
    incarnation: int
    start_time: float
    confirmers: set[tuple[str, int]] = field(default_factory=set)
    min_timeout: float = 1.0
    max_timeout: float = 10.0
    n_members: int = 1
    required_confirmations: int | None = None
    # Lifeguard re-gossip factor K: number of times to re-gossip suspicion
    regossip_factor: int = 3
    regossip_count: int = 0
    _timer_task: asyncio.Task | None = field(default=None, repr=False)
    
    # Memory bounds
    max_confirmers: int = MAX_CONFIRMERS
    _confirmations_dropped: int = 0
    # Track actual confirmation count even if we don't store all confirmers
    _logical_confirmation_count: int = 0
    
    def add_confirmation(self, from_node: tuple[str, int]) -> bool:
        """
        Add a confirmation from another node.
        Returns True if this is a new confirmation.
        
        If confirmers set is at max capacity, the confirmation is still
        counted for timeout calculation but the confirmer is not stored.
        """
        # Check if already confirmed
        if from_node in self.confirmers:
            return False
        
        # Track logical count for timeout calculation
        self._logical_confirmation_count += 1
        
        # Only store if under limit
        if len(self.confirmers) < self.max_confirmers:
            self.confirmers.add(from_node)
        else:
            self._confirmations_dropped += 1
        
        return True
    
    @property
    def confirmation_count(self) -> int:
        """
        Number of independent confirmations received.
        
        Uses logical count which may be higher than stored confirmers
        if we hit the max_confirmers limit.
        """
        return max(len(self.confirmers), self._logical_confirmation_count)
    
    def calculate_timeout(self) -> float:
        """
        Calculate the current suspicion timeout based on confirmations.
        
        Uses the Lifeguard/memberlist formula:
        timeout = max - (max - min) * log(C + 1) / log(K + 1)

        More confirmations = lower timeout (faster declaration of failure)
        once the configured confirmation target ``K`` is reached. If
        ``required_confirmations`` is omitted, the historical member-count
        denominator is retained for callers that have not opted into the
        bounded confirmation target.
        """
        c = self.confirmation_count
        confirmation_target = self.required_confirmations
        if confirmation_target is None:
            confirmation_target = self.n_members
        k = max(0, confirmation_target)

        if k <= 0:
            return self.min_timeout
        if c <= 0:
            return self.max_timeout

        bounded_confirmations = min(c, k)
        log_factor = math.log(bounded_confirmations + 1) / math.log(k + 1)
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
    
    def cleanup(self) -> None:
        """Clean up resources held by this suspicion state."""
        self.cancel_timer()
        self.confirmers.clear()
        self._logical_confirmation_count = 0
    
    def get_memory_stats(self) -> dict[str, int]:
        """Get memory-related statistics."""
        return {
            'confirmers_stored': len(self.confirmers),
            'confirmations_logical': self._logical_confirmation_count,
            'confirmations_dropped': self._confirmations_dropped,
            'max_confirmers': self.max_confirmers,
        }
